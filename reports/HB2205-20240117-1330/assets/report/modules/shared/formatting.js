import { toFiniteNumberOrNull } from "./coercion.js";

export function formatPercent(value, digits) {
  const parsed = toFiniteNumberOrNull(value);
  if (parsed === null) {
    return "-";
  }
  const precision = Number.isFinite(digits) ? digits : 1;
  return (parsed * 100).toFixed(precision) + "%";
}

export function formatRatio(numerator, denominator, digits) {
  const num = toFiniteNumberOrNull(numerator);
  const den = toFiniteNumberOrNull(denominator);
  if (num === null || den === null || den <= 0) {
    return "-";
  }
  const precision = Number.isFinite(digits) ? digits : 1;
  return ((num / den) * 100).toFixed(precision) + "%";
}

export function formatDurationHumanized(durationMs) {
  if (!Number.isFinite(durationMs) || durationMs <= 0) {
    return "";
  }
  const dayMs = 24 * 60 * 60 * 1000;
  const hourMs = 60 * 60 * 1000;
  const minuteMs = 60 * 1000;
  const days = Math.floor(durationMs / dayMs);
  const hours = Math.floor((durationMs % dayMs) / hourMs);
  const minutes = Math.floor((durationMs % hourMs) / minuteMs);
  const parts = [];
  if (days > 0) {
    parts.push(String(days) + "d");
  }
  if (hours > 0) {
    parts.push(String(hours) + "h");
  }
  if (minutes > 0 && days === 0) {
    parts.push(String(minutes) + "m");
  }
  return parts.join(" ");
}
