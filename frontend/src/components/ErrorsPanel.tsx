import { useState, useEffect, useCallback } from "react";
import "./ErrorsPanel.css";

interface FieldError {
  source: string;
  record_id: string;
  field_name: string;
  field_value: string;
  error_type: string;
  error_message: string;
  detected_at: string;
}

interface ErrorsResponse {
  total: number;
  page: number;
  page_size: number;
  errors: FieldError[];
  summary: Record<string, number>;
}

const SOURCE_LABELS: Record<string, string> = {
  sites: "Sites",
  circuit_details: "Circuit Details",
  bh_traffic: "BH Traffic",
  circuit_traffic: "Circuit Traffic",
};

const ERROR_TYPE_LABELS: Record<string, string> = {
  null: "Null / Missing",
  invalid_range: "Invalid Range",
  invalid_format: "Invalid Format",
  invalid_value: "Invalid Value",
};

const PAGE_SIZE = 25;

export function ErrorsPanel() {
  const [data, setData] = useState<ErrorsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [fetchError, setFetchError] = useState<string | null>(null);
  const [sourceFilter, setSourceFilter] = useState("");
  const [typeFilter, setTypeFilter] = useState("");
  const [page, setPage] = useState(1);

  const fetchErrors = useCallback((currentPage = page) => {
    setLoading(true);
    setFetchError(null);
    const params = new URLSearchParams({
      page: String(currentPage),
      page_size: String(PAGE_SIZE),
    });
    if (sourceFilter) params.set("source", sourceFilter);
    if (typeFilter) params.set("error_type", typeFilter);

    fetch(`/api/errors?${params}`)
      .then((r) => {
        if (!r.ok) throw new Error(`Server returned ${r.status}`);
        return r.json();
      })
      .then((d: ErrorsResponse) => {
        setData(d);
        setLoading(false);
      })
      .catch((e) => {
        setFetchError(e.message);
        setLoading(false);
      });
  }, [sourceFilter, typeFilter, page]);

  useEffect(() => {
    fetchErrors(page);
  }, [sourceFilter, typeFilter, page]);

  const handleRefresh = () => {
    setRefreshing(true);
    fetch("/api/errors/refresh", { method: "POST" })
      .then(() => {
        setPage(1);
        fetchErrors(1);
      })
      .finally(() => setRefreshing(false));
  };

  const handleSourceFilter = (src: string) => {
    setSourceFilter((prev) => (prev === src ? "" : src));
    setPage(1);
  };

  const totalPages = data ? Math.ceil(data.total / PAGE_SIZE) : 1;

  return (
    <div className="ep-panel">
      <div className="ep-header">
        <h2 className="ep-title">Data Quality — Field Errors</h2>
        <button
          className="ep-btn ep-btn-primary"
          onClick={handleRefresh}
          disabled={refreshing}
        >
          {refreshing ? "Refreshing…" : "Refresh"}
        </button>
      </div>

      {/* Summary cards */}
      {data && (
        <div className="ep-summary">
          {Object.keys(SOURCE_LABELS).map((src) => {
            const count = data.summary[src] ?? 0;
            return (
              <button
                key={src}
                className={`ep-card ${sourceFilter === src ? "ep-card--active" : ""} ${count === 0 ? "ep-card--clean" : "ep-card--errors"}`}
                onClick={() => handleSourceFilter(src)}
              >
                <span className="ep-card-count">{count}</span>
                <span className="ep-card-label">{SOURCE_LABELS[src]}</span>
              </button>
            );
          })}
        </div>
      )}

      {/* Filters */}
      <div className="ep-filters">
        <select
          value={sourceFilter}
          onChange={(e) => { setSourceFilter(e.target.value); setPage(1); }}
          className="ep-select"
        >
          <option value="">All Sources</option>
          {Object.entries(SOURCE_LABELS).map(([k, v]) => (
            <option key={k} value={k}>{v}</option>
          ))}
        </select>

        <select
          value={typeFilter}
          onChange={(e) => { setTypeFilter(e.target.value); setPage(1); }}
          className="ep-select"
        >
          <option value="">All Error Types</option>
          {Object.entries(ERROR_TYPE_LABELS).map(([k, v]) => (
            <option key={k} value={k}>{v}</option>
          ))}
        </select>

        {(sourceFilter || typeFilter) && (
          <button
            className="ep-btn ep-btn-ghost"
            onClick={() => { setSourceFilter(""); setTypeFilter(""); setPage(1); }}
          >
            Clear Filters
          </button>
        )}

        {data && (
          <span className="ep-total">
            {data.total} error{data.total !== 1 ? "s" : ""}
          </span>
        )}
      </div>

      {/* Table */}
      {loading ? (
        <p className="ep-state-msg">Loading…</p>
      ) : fetchError ? (
        <p className="ep-state-msg ep-state-msg--error">{fetchError}</p>
      ) : data && data.errors.length > 0 ? (
        <>
          <div className="ep-table-wrap">
            <table className="ep-table">
              <thead>
                <tr>
                  <th>Source</th>
                  <th>Record ID</th>
                  <th>Field</th>
                  <th>Value</th>
                  <th>Type</th>
                  <th>Message</th>
                </tr>
              </thead>
              <tbody>
                {data.errors.map((err, i) => (
                  <tr key={i}>
                    <td>
                      <span className="ep-source-tag">
                        {SOURCE_LABELS[err.source] ?? err.source}
                      </span>
                    </td>
                    <td className="ep-mono">{err.record_id}</td>
                    <td>{err.field_name}</td>
                    <td className="ep-mono ep-value">
                      {err.field_value || <em className="ep-empty">empty</em>}
                    </td>
                    <td>
                      <span className={`ep-badge ep-badge--${err.error_type}`}>
                        {err.error_type.replace("_", " ")}
                      </span>
                    </td>
                    <td className="ep-message">{err.error_message}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {totalPages > 1 && (
            <div className="ep-pagination">
              <button
                className="ep-btn ep-btn-ghost"
                disabled={page === 1}
                onClick={() => setPage(page - 1)}
              >
                ← Previous
              </button>
              <span className="ep-page-info">
                Page {page} of {totalPages}
              </span>
              <button
                className="ep-btn ep-btn-ghost"
                disabled={page === totalPages}
                onClick={() => setPage(page + 1)}
              >
                Next →
              </button>
            </div>
          )}
        </>
      ) : (
        <p className="ep-state-msg ep-state-msg--clean">
          No errors found{sourceFilter || typeFilter ? " for the current filters" : ""}.
        </p>
      )}
    </div>
  );
}
