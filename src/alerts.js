import {choice, digest, exactKeys, identifier, integer, number, text, timestamp} from './value.js';
import {requireValue} from './errors.js';
import {metric} from './catalog.js';
import {aggregateWindows} from './windows.js';
import {event} from './state.js';

export function ruleInput(input) {
  exactKeys(input, ['id', 'station', 'metric', 'widthMs', 'statistic', 'operator', 'threshold', 'minimumCompleteness', 'finalOnly'],
    ['id', 'station', 'metric', 'widthMs', 'statistic', 'operator', 'threshold']);
  metric(input.metric);
  if (input.finalOnly !== undefined) {
    requireValue(typeof input.finalOnly === 'boolean', 'BOOLEAN', 'finalOnly must be boolean');
  }
  return {
    id: identifier(input.id),
    station: identifier(input.station, 'station'),
    metric: input.metric,
    widthMs: integer(input.widthMs, 'widthMs', 1000, 604800000),
    statistic: choice(input.statistic, ['sum', 'mean', 'min', 'max', 'variance', 'completeness'], 'statistic'),
    operator: choice(input.operator, ['gt', 'gte', 'lt', 'lte'], 'operator'),
    threshold: number(input.threshold, 'threshold'),
    minimumCompleteness: number(input.minimumCompleteness ?? 0, 'minimumCompleteness', 0, 1),
    finalOnly: input.finalOnly ?? false,
    enabled: true,
  };
}
export function saveRule(state, input) {
  const rule = ruleInput(input);
  requireValue(state.stations[rule.station], 'STATION_MISSING', 'Rule station does not exist');
  const old = state.rules[rule.id];
  rule.version = old ? old.version + 1 : 1;
  return {events: [event('rule.saved', rule)], result: {rule}};
}
export function disableRule(state, id) {
  identifier(id);
  requireValue(state.rules[id], 'RULE_MISSING', 'Rule does not exist');
  const events = [event('rule.disabled', {id})];
  for (const alert of Object.values(state.alerts)) {
    if (alert.rule === id && alert.active) {
      events.push(event('alert.updated', {...alert, active: false, reason: 'rule_disabled', generation: alert.generation + 1}));
    }
  }
  return {events, result: {id, disabled: true}};
}
function crossed(value, operator, threshold) {
  if (value === null) {
    return false;
  }
  switch (operator) {
    case 'gt':
      return value > threshold;
    case 'gte':
      return value >= threshold;
    case 'lt':
      return value < threshold;
    case 'lte':
      return value <= threshold;
    default:
      return false;
  }
}
export function evaluateAlerts(state, input) {
  exactKeys(input, ['from', 'until', 'rule'], ['from', 'until']);
  const from = timestamp(input.from);
  const until = timestamp(input.until);
  requireValue(from < until, 'PERIOD', 'Evaluation period is invalid');
  if (input.rule) {
    identifier(input.rule);
    requireValue(state.rules[input.rule], 'RULE_MISSING', 'Rule does not exist');
  }
  const events = [];
  const result = {opened: 0, retracted: 0, changed: 0, unchanged: 0, evaluated: 0};
  const rules = Object.values(state.rules).filter(rule => rule.enabled && (!input.rule || input.rule === rule.id));
  for (const rule of rules) {
    requireValue(Date.parse(from) % rule.widthMs === 0, 'WINDOW_ALIGNMENT', 'Alert evaluation start must align to epoch window');
    requireValue(Date.parse(until) % rule.widthMs === 0, 'WINDOW_ALIGNMENT', 'Alert evaluation end must align to epoch window');
    const report = aggregateWindows(state, {station: rule.station, metric: rule.metric, from, until, widthMs: rule.widthMs});
    for (const window of report.windows) {
      const id = digest({rule: rule.id, from: window.from}).slice(0, 32);
      const previous = state.alerts[id];
      const eligible = window.completeness >= rule.minimumCompleteness && (!rule.finalOnly || window.final);
      const active = eligible && crossed(window[rule.statistic], rule.operator, rule.threshold);
      result.evaluated += 1;
      if (!previous && !active) {
        result.unchanged += 1;
        continue;
      }
      const fingerprint = digest({data: window.fingerprint, active, ruleVersion: rule.version, final: window.final});
      if (previous?.fingerprint === fingerprint) {
        result.unchanged += 1;
        continue;
      }
      const alert = {
        id,
        rule: rule.id,
        ruleVersion: rule.version,
        station: rule.station,
        metric: rule.metric,
        from: window.from,
        until: window.until,
        value: window[rule.statistic],
        threshold: rule.threshold,
        operator: rule.operator,
        active,
        final: window.final,
        completeness: window.completeness,
        fingerprint,
        lineage: window.lineage,
        generation: (previous?.generation ?? 0) + 1,
        acknowledgement: previous?.acknowledgement ?? null,
        reason: active ? 'threshold_crossed' : eligible ? 'corrected_below_threshold' : 'insufficient_data',
      };
      events.push(event('alert.updated', alert));
      if (active && !previous?.active) {
        result.opened += 1;
      } else if (!active && previous?.active) {
        result.retracted += 1;
      } else {
        result.changed += 1;
      }
    }
  }
  return {events, result};
}
export function acknowledgeAlert(state, input) {
  exactKeys(input, ['id', 'actor', 'note', 'generation'], ['id', 'actor', 'note', 'generation']);
  const id = identifier(input.id);
  const alert = state.alerts[id];
  requireValue(alert, 'ALERT_MISSING', 'Alert does not exist');
  const generation = integer(input.generation, 'generation', 1);
  requireValue(generation === alert.generation, 'ALERT_CHANGED', 'Alert changed before acknowledgement');
  const acknowledgement = {id, actor: identifier(input.actor), note: text(input.note, 'note', 1000), generation};
  return {events: [event('alert.acknowledged', acknowledgement)], result: acknowledgement};
}
export function listAlerts(state, {active, station, rule} = {}) {
  return Object.values(state.alerts).filter(alert => {
    if (active !== undefined && alert.active !== active) {
      return false;
    }
    if (station && alert.station !== station) {
      return false;
    }
    return !rule || alert.rule === rule;
  }).sort((a, b) => a.from.localeCompare(b.from) || a.id.localeCompare(b.id));
}
