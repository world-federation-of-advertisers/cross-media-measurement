export enum ReportState {
  STATE_UNSPECIFIED,
  RUNNING,
  SUCCEEDED,
  FAILED,
};

export const terminatingStates = Object.freeze([ReportState.SUCCEEDED, ReportState.FAILED]);
