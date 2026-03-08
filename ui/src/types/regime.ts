import type { RegimeCode } from '@/types/strategy';

export type RegimeStatus = 'confirmed' | 'transition' | 'unclassified';
export type TrendState = 'positive' | 'negative' | 'near_zero';
export type CurveState = 'contango' | 'flat' | 'inverted';

export interface RegimeSnapshot {
  as_of_date: string;
  effective_from_date: string;
  model_name: string;
  model_version: number;
  regime_code: RegimeCode;
  regime_status: RegimeStatus;
  matched_rule_id?: string | null;
  halt_flag: boolean;
  halt_reason?: string | null;
  spy_return_20d?: number | null;
  rvol_10d_ann?: number | null;
  vix_spot_close?: number | null;
  vix3m_close?: number | null;
  vix_slope?: number | null;
  trend_state?: TrendState | null;
  curve_state?: CurveState | null;
  vix_gt_32_streak?: number | null;
  computed_at?: string | null;
}

export interface RegimeInputRow {
  as_of_date: string;
  spy_close?: number | null;
  return_1d?: number | null;
  return_20d?: number | null;
  rvol_10d_ann?: number | null;
  vix_spot_close?: number | null;
  vix3m_close?: number | null;
  vix_slope?: number | null;
  trend_state?: TrendState | null;
  curve_state?: CurveState | null;
  vix_gt_32_streak?: number | null;
  inputs_complete_flag: boolean;
  computed_at?: string | null;
}

export interface RegimeTransitionRow {
  model_name: string;
  model_version: number;
  effective_from_date: string;
  prior_regime_code?: RegimeCode | null;
  new_regime_code: RegimeCode;
  trigger_rule_id?: string | null;
  computed_at?: string | null;
}

export interface RegimeModelSummary {
  name: string;
  description?: string;
  version: number;
  updated_at?: string | null;
  active_version?: number | null;
  activated_at?: string | null;
  activated_by?: string | null;
}

export interface RegimeModelRevision {
  name: string;
  version: number;
  description?: string;
  config: Record<string, unknown>;
  status?: string | null;
  config_hash?: string | null;
  published_at?: string | null;
  created_at?: string | null;
  activated_at?: string | null;
  activated_by?: string | null;
}

export interface RegimeModelDetailResponse {
  model: RegimeModelSummary;
  activeRevision?: RegimeModelRevision | null;
  revisions: RegimeModelRevision[];
  latest?: RegimeSnapshot | null;
}
