#!/usr/bin/env bash
# Local Kubernetes for the shortener: a minikube profile "shortener" running
# k8s/overlays/local, with the dependencies from `make up` on the host.
#
#   scripts/k8s.sh up        start the minikube profile
#   scripts/k8s.sh deploy    build the image, load it, apply, wait for rollout
#   scripts/k8s.sh smoke     run the smoke test through a port-forward
#   scripts/k8s.sh rollout   restart the shortener pods under load from inside
#                            the cluster and require no failed requests
#   scripts/k8s.sh down      delete the namespace (the cluster keeps running)
#   scripts/k8s.sh destroy   delete the minikube profile
#
# Every kubectl call names the context explicitly: the current context may
# point at any cluster.
set -euo pipefail

profile=shortener
ctx=(--context "$profile")
ns=(--namespace shortener)
image=shortener:dev

case "${1:-}" in
up)
	minikube start -p "$profile" --driver=docker --memory=2048 --cpus=2 --addons=metrics-server
	;;
deploy)
	docker build -q -t "$image" . >/dev/null
	minikube -p "$profile" image load --overwrite "$image"
	kubectl "${ctx[@]}" apply -k k8s/overlays/local
	# The tag does not change between builds, so restart to pick up the image.
	kubectl "${ctx[@]}" "${ns[@]}" rollout restart deploy/shortener deploy/consumer
	kubectl "${ctx[@]}" "${ns[@]}" rollout status deploy/shortener --timeout=180s
	kubectl "${ctx[@]}" "${ns[@]}" rollout status deploy/consumer --timeout=180s
	;;
smoke)
	# One port-forward for both ports, so they reach the same pod.
	kubectl "${ctx[@]}" "${ns[@]}" port-forward svc/shortener 18080:80 19091:9091 >/dev/null 2>&1 &
	pf=$!
	trap 'kill "$pf" 2>/dev/null || true' EXIT
	for _ in $(seq 1 30); do
		curl -sf http://127.0.0.1:18080/healthz >/dev/null 2>&1 && break
		sleep 0.5
	done
	SMOKE_URL=http://127.0.0.1:18080 SMOKE_METRICS_URL=http://127.0.0.1:19091 SMOKE_ANALYTICS=1 \
		./scripts/smoke.sh
	;;
rollout)
	# A link to request: created through a short-lived port-forward.
	kubectl "${ctx[@]}" "${ns[@]}" port-forward svc/shortener 18080:80 >/dev/null 2>&1 &
	pf=$!
	for _ in $(seq 1 30); do curl -sf http://127.0.0.1:18080/healthz >/dev/null 2>&1 && break; sleep 0.5; done
	alias="rollout$(date +%s)"
	body="{\"long_url\":\"https://example.com/rollout\",\"alias\":\"$alias\"}"
	curl -sf -o /dev/null -X POST http://127.0.0.1:18080/api/v1/links -d "$body"
	kill "$pf"

	# The load must come from inside the cluster, through the Service: a
	# port-forward is bound to one pod and breaks when that pod restarts.
	docker build -q -t shortener-loadgen:dev -f- . >/dev/null <<'DOCKERFILE'
FROM golang:1.26 AS build
RUN CGO_ENABLED=0 GOBIN=/out go install github.com/tsenart/vegeta/v12@v12.13.0
FROM busybox:1.37
COPY --from=build /out/vegeta /usr/local/bin/vegeta
DOCKERFILE
	minikube -p "$profile" image load --overwrite shortener-loadgen:dev
	kubectl "${ctx[@]}" "${ns[@]}" delete pod loadgen --ignore-not-found --wait >/dev/null
	kubectl "${ctx[@]}" "${ns[@]}" run loadgen --image=shortener-loadgen:dev --image-pull-policy=Never \
		--restart=Never --command -- sh -c \
		"echo 'GET http://shortener/$alias' | vegeta attack -rate=200 -duration=45s -redirects=-1 | vegeta report"
	kubectl "${ctx[@]}" "${ns[@]}" wait --for=condition=Ready pod/loadgen --timeout=60s >/dev/null
	sleep 8 # steady load first
	echo "rollout: restarting the shortener pods under 200 requests/s"
	kubectl "${ctx[@]}" "${ns[@]}" rollout restart deploy/shortener
	kubectl "${ctx[@]}" "${ns[@]}" rollout status deploy/shortener --timeout=180s
	kubectl "${ctx[@]}" "${ns[@]}" wait --for=jsonpath='{.status.phase}'=Succeeded pod/loadgen --timeout=120s >/dev/null
	report="$(kubectl "${ctx[@]}" "${ns[@]}" logs loadgen)"
	kubectl "${ctx[@]}" "${ns[@]}" delete pod loadgen --wait=false >/dev/null
	echo "$report" | grep -E '^(Requests|Latencies|Success|Status Codes)'
	if ! echo "$report" | grep -qE '^Success +\[ratio\] +100\.00%'; then
		echo "rollout: FAIL: requests failed during the rolling restart" >&2
		exit 1
	fi
	echo "rollout: ok   no failed requests during the rolling restart"
	;;
down)
	kubectl "${ctx[@]}" delete namespace shortener --ignore-not-found --wait
	;;
destroy)
	minikube delete -p "$profile"
	;;
*)
	echo "usage: $0 up|deploy|smoke|rollout|down|destroy" >&2
	exit 2
	;;
esac
