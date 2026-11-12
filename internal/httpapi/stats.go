package httpapi

import (
	"net/http"
	"time"

	"github.com/pristupaanastasia/shortener/internal/analytics"
)

// Stats query limits.
const (
	defaultStatsDays = 30
	maxStatsDays     = 366
)

type statsResponse struct {
	TotalClicks uint64       `json:"total_clicks"`
	Timeseries  []dayClicks  `json:"timeseries"`
	ByCountry   []valueCount `json:"by_country"`
	ByDevice    []valueCount `json:"by_device"`
	ByReferrer  []valueCount `json:"by_referrer"`
	// UpdatedAt is the time of the latest ingested click (null if none).
	UpdatedAt *time.Time `json:"updated_at"`
}

type dayClicks struct {
	Date   string `json:"date"`
	Clicks uint64 `json:"clicks"`
}

type valueCount struct {
	Value  string `json:"value"`
	Clicks uint64 `json:"clicks"`
}

// linkStats serves GET /api/v1/links/{key}/stats?from=YYYY-MM-DD&to=YYYY-MM-DD.
// Days are UTC; the range defaults to the last 30 days.
func (h *Handler) linkStats(w http.ResponseWriter, r *http.Request) {
	if h.stats == nil {
		writeError(w, http.StatusServiceUnavailable, "analytics_unavailable", "analytics is not configured")
		return
	}
	if _, err := h.links.Get(r.Context(), userFrom(r.Context()), r.PathValue("key")); err != nil {
		h.writeLinkError(w, "stats", err)
		return
	}

	q := r.URL.Query()
	if g := q.Get("granularity"); g != "" && g != "day" {
		writeError(w, http.StatusBadRequest, "invalid_request", "granularity must be day")
		return
	}
	today := h.now().UTC().Truncate(24 * time.Hour)
	to, ok := parseDay(q.Get("to"), today)
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid_request", "to must be a date (YYYY-MM-DD)")
		return
	}
	from, ok := parseDay(q.Get("from"), to.AddDate(0, 0, -(defaultStatsDays-1)))
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid_request", "from must be a date (YYYY-MM-DD)")
		return
	}
	if from.After(to) || to.Sub(from) >= maxStatsDays*24*time.Hour {
		writeError(w, http.StatusBadRequest, "invalid_request", "from must not be after to, and the range must be at most 366 days")
		return
	}

	st, err := h.stats.Stats(r.Context(), r.PathValue("key"), from, to)
	if err != nil {
		h.logger.Error("stats", "err", err)
		writeError(w, http.StatusServiceUnavailable, "analytics_unavailable", "statistics are temporarily unavailable")
		return
	}
	writeJSON(w, http.StatusOK, statsView(st))
}

func parseDay(s string, fallback time.Time) (time.Time, bool) {
	if s == "" {
		return fallback, true
	}
	d, err := time.Parse(time.DateOnly, s)
	return d, err == nil
}

func statsView(st analytics.Stats) statsResponse {
	resp := statsResponse{
		TotalClicks: st.Total,
		Timeseries:  make([]dayClicks, 0, len(st.Timeseries)),
		ByCountry:   values(st.ByCountry),
		ByDevice:    values(st.ByDevice),
		ByReferrer:  values(st.ByReferrer),
	}
	for _, d := range st.Timeseries {
		resp.Timeseries = append(resp.Timeseries, dayClicks{Date: d.Day.Format(time.DateOnly), Clicks: d.Clicks})
	}
	if !st.LastClick.IsZero() {
		t := st.LastClick.UTC()
		resp.UpdatedAt = &t
	}
	return resp
}

func values(vs []analytics.ValueCount) []valueCount {
	out := make([]valueCount, 0, len(vs))
	for _, v := range vs {
		out = append(out, valueCount{Value: v.Value, Clicks: v.Clicks})
	}
	return out
}
