import { useEffect, useState } from 'react';
import { useFieldArray, useForm } from 'react-hook-form';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { ArrowDown, ArrowUp, Plus, Trash2 } from 'lucide-react';
import { Button } from '@/app/components/ui/button';
import { Input } from '@/app/components/ui/input';
import { Label } from '@/app/components/ui/label';
import { Switch } from '@/app/components/ui/switch';
import { PageLoader } from '@/app/components/common/PageLoader';
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetFooter,
  SheetHeader,
  SheetTitle
} from '@/app/components/ui/sheet';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue
} from '@/app/components/ui/select';
import { strategyApi } from '@/services/strategyApi';
import { toast } from 'sonner';
import { formatSystemStatusText } from '@/utils/formatSystemStatusText';
import type {
  ExitRule,
  ExitRulePriceField,
  ExitRuleType,
  IntrabarConflictPolicy,
  StrategyDetail,
  StrategySummary
} from '@/types/strategy';

interface StrategyEditorProps {
  strategy: StrategySummary | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

const EXIT_RULE_OPTIONS: Array<{ value: ExitRuleType; label: string }> = [
  { value: 'stop_loss_fixed', label: 'Fixed Stop Loss' },
  { value: 'take_profit_fixed', label: 'Fixed Take Profit' },
  { value: 'trailing_stop_pct', label: 'Trailing Stop %' },
  { value: 'trailing_stop_atr', label: 'Trailing Stop ATR' },
  { value: 'time_stop', label: 'Time Stop' }
];

const PRICE_FIELD_OPTIONS: Array<{ value: ExitRulePriceField; label: string }> = [
  { value: 'open', label: 'Open' },
  { value: 'high', label: 'High' },
  { value: 'low', label: 'Low' },
  { value: 'close', label: 'Close' }
];

const INTRABAR_OPTIONS: Array<{ value: IntrabarConflictPolicy; label: string }> = [
  { value: 'stop_first', label: 'Stop First' },
  { value: 'take_profit_first', label: 'Take Profit First' },
  { value: 'priority_order', label: 'Priority Order' }
];

function buildEmptyStrategy(): StrategyDetail {
  return {
    name: '',
    type: 'configured',
    description: '',
    config: {
      universe: 'SP500',
      rebalance: 'monthly',
      longOnly: true,
      topN: 20,
      lookbackWindow: 63,
      holdingPeriod: 21,
      costModel: 'default',
      intrabarConflictPolicy: 'stop_first',
      exits: []
    }
  };
}

function getNextRuleId(type: ExitRuleType, existingRules: ExitRule[]): string {
  const used = new Set(existingRules.map((rule) => rule.id));
  let counter = 1;
  while (used.has(`${type}-${counter}`)) {
    counter += 1;
  }
  return `${type}-${counter}`;
}

function buildExitRule(type: ExitRuleType, id: string, overrides: Partial<ExitRule> = {}): ExitRule {
  const baseRule: ExitRule = {
    id,
    enabled: true,
    type,
    scope: 'position',
    action: 'exit_full',
    minHoldBars: 0,
    priority: 0
  };

  if (type === 'stop_loss_fixed') {
    return {
      ...baseRule,
      value: 0.08,
      reference: 'entry_price',
      priceField: 'low',
      ...overrides
    };
  }

  if (type === 'take_profit_fixed') {
    return {
      ...baseRule,
      value: 0.15,
      reference: 'entry_price',
      priceField: 'high',
      ...overrides
    };
  }

  if (type === 'trailing_stop_pct') {
    return {
      ...baseRule,
      value: 0.07,
      reference: 'highest_since_entry',
      priceField: 'low',
      ...overrides
    };
  }

  if (type === 'trailing_stop_atr') {
    return {
      ...baseRule,
      value: 3,
      atrColumn: 'atr_14d',
      reference: 'highest_since_entry',
      priceField: 'low',
      ...overrides
    };
  }

  return {
    ...baseRule,
    value: 40,
    priceField: 'close',
    ...overrides
  };
}

function getRuleValueLabel(type: ExitRuleType): string {
  if (type === 'time_stop') return 'Bars';
  if (type === 'trailing_stop_atr') return 'ATR Multiple';
  return 'Value';
}

function toOptionalNumber(value: string): number | undefined {
  const trimmed = String(value).trim();
  if (!trimmed) return undefined;
  const parsed = Number(trimmed);
  return Number.isFinite(parsed) ? parsed : undefined;
}

export function StrategyEditor({ strategy, open, onOpenChange }: StrategyEditorProps) {
  const queryClient = useQueryClient();
  const isEditing = Boolean(strategy?.name);
  const [newRuleType, setNewRuleType] = useState<ExitRuleType>('stop_loss_fixed');

  const detailQuery = useQuery({
    queryKey: ['strategies', 'detail', strategy?.name],
    queryFn: () => strategyApi.getStrategyDetail(String(strategy?.name)),
    enabled: open && isEditing
  });

  const {
    register,
    handleSubmit,
    reset,
    setValue,
    watch,
    getValues,
    control,
    formState: { errors }
  } = useForm<StrategyDetail>({
    defaultValues: buildEmptyStrategy()
  });

  const { fields, append, remove, move, update } = useFieldArray({
    control,
    name: 'config.exits'
  });

  useEffect(() => {
    if (!open) return;
    if (isEditing) {
      if (detailQuery.data) {
        reset(detailQuery.data);
      }
      return;
    }
    reset(buildEmptyStrategy());
  }, [detailQuery.data, isEditing, open, reset]);

  const mutation = useMutation({
    mutationFn: (data: StrategyDetail) => strategyApi.saveStrategy(data),
    onSuccess: async (_, savedStrategy) => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ['strategies'] }),
        queryClient.invalidateQueries({ queryKey: ['strategies', 'detail', savedStrategy.name] })
      ]);
      toast.success(`Strategy ${isEditing ? 'updated' : 'created'} successfully`);
      onOpenChange(false);
    },
    onError: (error) => {
      toast.error(`Failed to save strategy: ${formatSystemStatusText(error)}`);
    }
  });

  const onSubmit = (data: StrategyDetail) => {
    mutation.mutate(data);
  };

  const watchedType = watch('type');
  const watchedRebalance = watch('config.rebalance');
  const watchedPolicy = watch('config.intrabarConflictPolicy');
  const watchedLongOnly = watch('config.longOnly');
  const watchedExits = watch('config.exits') || [];

  const addExitRule = () => {
    const existingRules = getValues('config.exits') || [];
    const newRuleId = getNextRuleId(newRuleType, existingRules);
    append(
      buildExitRule(newRuleType, newRuleId, {
        priority: existingRules.length
      })
    );
  };

  const changeRuleType = (index: number, nextType: ExitRuleType) => {
    const currentRule = getValues(`config.exits.${index}` as const);
    update(
      index,
      buildExitRule(nextType, currentRule.id, {
        enabled: currentRule.enabled,
        minHoldBars: currentRule.minHoldBars,
        priority: currentRule.priority
      })
    );
  };

  const detailError = isEditing ? formatSystemStatusText(detailQuery.error) : '';

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="overflow-y-auto sm:max-w-4xl">
        <SheetHeader>
          <SheetTitle>{isEditing ? 'Edit Strategy' : 'New Strategy'}</SheetTitle>
          <SheetDescription>
            Configure strategy parameters, exit rules, and intrabar conflict handling.
          </SheetDescription>
        </SheetHeader>

        {isEditing && detailQuery.isLoading ? (
          <PageLoader text="Loading strategy..." className="h-72" />
        ) : isEditing && detailError ? (
          <div className="rounded-xl border border-destructive/30 bg-destructive/10 p-4 text-sm text-destructive">
            {detailError}
          </div>
        ) : (
          <form onSubmit={handleSubmit(onSubmit)} className="space-y-6 py-4">
            <div className="space-y-4">
              <h3 className="text-sm font-medium text-muted-foreground">Metadata</h3>
              <div className="grid gap-4 md:grid-cols-2">
                <div className="grid gap-2">
                  <Label htmlFor="name">Name</Label>
                  <Input
                    id="name"
                    readOnly={isEditing}
                    {...register('name', { required: true })}
                    placeholder="e.g. mom-spy-res"
                  />
                  {errors.name && <span className="text-xs text-red-500">Name is required</span>}
                </div>

                <div className="grid gap-2">
                  <Label htmlFor="type">Type</Label>
                  <Select
                    value={watchedType}
                    onValueChange={(value) =>
                      setValue('type', value, { shouldDirty: true, shouldTouch: true })
                    }
                  >
                    <SelectTrigger id="type">
                      <SelectValue placeholder="Select type" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="configured">Configured</SelectItem>
                      <SelectItem value="code-based">Code Based</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>

              <div className="grid gap-2">
                <Label htmlFor="description">Description</Label>
                <Input id="description" {...register('description')} />
              </div>
            </div>

            <div className="space-y-4 border-t pt-4">
              <h3 className="text-sm font-medium text-muted-foreground">Configuration</h3>

              <div className="grid gap-4 md:grid-cols-2">
                <div className="grid gap-2">
                  <Label htmlFor="universe">Universe</Label>
                  <Input
                    id="universe"
                    {...register('config.universe', { required: true })}
                    placeholder="SP500, NDX, etc."
                  />
                </div>

                <div className="grid gap-2">
                  <Label htmlFor="rebalance">Rebalance Frequency</Label>
                  <Select
                    value={watchedRebalance}
                    onValueChange={(value) =>
                      setValue('config.rebalance', value, { shouldDirty: true, shouldTouch: true })
                    }
                  >
                    <SelectTrigger id="rebalance">
                      <SelectValue placeholder="Select frequency" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="daily">Daily</SelectItem>
                      <SelectItem value="weekly">Weekly</SelectItem>
                      <SelectItem value="monthly">Monthly</SelectItem>
                      <SelectItem value="quarterly">Quarterly</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>

              <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
                <div className="grid gap-2">
                  <Label htmlFor="top-n">Top N</Label>
                  <Input
                    id="top-n"
                    type="number"
                    {...register('config.topN', { valueAsNumber: true })}
                  />
                </div>
                <div className="grid gap-2">
                  <Label htmlFor="lookback">Lookback (Days)</Label>
                  <Input
                    id="lookback"
                    type="number"
                    {...register('config.lookbackWindow', { valueAsNumber: true })}
                  />
                </div>
                <div className="grid gap-2">
                  <Label htmlFor="holding">Holding (Days)</Label>
                  <Input
                    id="holding"
                    type="number"
                    {...register('config.holdingPeriod', { valueAsNumber: true })}
                  />
                </div>
                <div className="grid gap-2">
                  <Label htmlFor="cost-model">Cost Model</Label>
                  <Input id="cost-model" {...register('config.costModel')} />
                </div>
              </div>

              <div className="grid gap-4 md:grid-cols-[1fr_auto]">
                <div className="grid gap-2">
                  <Label htmlFor="intrabar-policy">Intrabar Conflict Policy</Label>
                  <Select
                    value={watchedPolicy}
                    onValueChange={(value) =>
                      setValue('config.intrabarConflictPolicy', value as IntrabarConflictPolicy, {
                        shouldDirty: true,
                        shouldTouch: true
                      })
                    }
                  >
                    <SelectTrigger id="intrabar-policy">
                      <SelectValue placeholder="Select policy" />
                    </SelectTrigger>
                    <SelectContent>
                      {INTRABAR_OPTIONS.map((option) => (
                        <SelectItem key={option.value} value={option.value}>
                          {option.label}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>

                <div className="flex items-end justify-between gap-3 rounded-xl border px-4 py-3">
                  <div>
                    <p className="text-sm font-medium">Long Only</p>
                    <p className="text-xs text-muted-foreground">Milestone 1 keeps exit scope to positions only.</p>
                  </div>
                  <Switch
                    aria-label="Toggle long only strategy"
                    checked={Boolean(watchedLongOnly)}
                    onCheckedChange={(checked) =>
                      setValue('config.longOnly', Boolean(checked), {
                        shouldDirty: true,
                        shouldTouch: true
                      })
                    }
                  />
                </div>
              </div>
            </div>

            <div className="space-y-4 border-t pt-4">
              <div className="flex flex-col gap-4 md:flex-row md:items-end md:justify-between">
                <div>
                  <h3 className="text-sm font-medium text-muted-foreground">Exit Rules</h3>
                  <p className="text-xs text-muted-foreground">
                    Milestone 1 supports position-scope full exits only. Array order is the tie-breaker when priorities match.
                  </p>
                </div>

                <div className="flex flex-col gap-2 sm:flex-row sm:items-center">
                  <Select value={newRuleType} onValueChange={(value) => setNewRuleType(value as ExitRuleType)}>
                    <SelectTrigger className="min-w-[220px]">
                      <SelectValue placeholder="Choose rule type" />
                    </SelectTrigger>
                    <SelectContent>
                      {EXIT_RULE_OPTIONS.map((option) => (
                        <SelectItem key={option.value} value={option.value}>
                          {option.label}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                  <Button type="button" onClick={addExitRule} className="gap-2">
                    <Plus className="h-4 w-4" />
                    Add Exit Rule
                  </Button>
                </div>
              </div>

              {fields.length === 0 ? (
                <div className="rounded-xl border border-dashed p-4 text-sm text-muted-foreground">
                  No exit rules configured yet.
                </div>
              ) : (
                <div className="space-y-4">
                  {fields.map((field, index) => {
                    const currentRule = watchedExits[index];
                    const ruleType = currentRule?.type || field.type;
                    const ruleValueLabel = getRuleValueLabel(ruleType);

                    return (
                      <div key={field.id} className="space-y-4 rounded-2xl border p-4">
                        <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
                          <div className="grid gap-2 md:min-w-[260px]">
                            <Label htmlFor={`exit-rule-type-${index}`}>Rule Type</Label>
                            <Select
                              value={ruleType}
                              onValueChange={(value) => changeRuleType(index, value as ExitRuleType)}
                            >
                              <SelectTrigger id={`exit-rule-type-${index}`}>
                                <SelectValue placeholder="Select rule type" />
                              </SelectTrigger>
                              <SelectContent>
                                {EXIT_RULE_OPTIONS.map((option) => (
                                  <SelectItem key={option.value} value={option.value}>
                                    {option.label}
                                  </SelectItem>
                                ))}
                              </SelectContent>
                            </Select>
                          </div>

                          <div className="flex items-center gap-2 self-end md:self-auto">
                            <Button
                              type="button"
                              variant="outline"
                              size="icon"
                              onClick={() => move(index, index - 1)}
                              disabled={index === 0}
                              aria-label={`Move exit rule ${index + 1} up`}
                            >
                              <ArrowUp className="h-4 w-4" />
                            </Button>
                            <Button
                              type="button"
                              variant="outline"
                              size="icon"
                              onClick={() => move(index, index + 1)}
                              disabled={index === fields.length - 1}
                              aria-label={`Move exit rule ${index + 1} down`}
                            >
                              <ArrowDown className="h-4 w-4" />
                            </Button>
                            <Button
                              type="button"
                              variant="outline"
                              size="icon"
                              onClick={() => remove(index)}
                              aria-label={`Remove exit rule ${index + 1}`}
                            >
                              <Trash2 className="h-4 w-4" />
                            </Button>
                          </div>
                        </div>

                        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
                          <div className="grid gap-2">
                            <Label htmlFor={`exit-rule-id-${index}`}>Rule ID</Label>
                            <Input
                              id={`exit-rule-id-${index}`}
                              {...register(`config.exits.${index}.id` as const, { required: true })}
                            />
                          </div>

                          <div className="grid gap-2">
                            <Label htmlFor={`exit-rule-priority-${index}`}>Priority</Label>
                            <Input
                              id={`exit-rule-priority-${index}`}
                              type="number"
                              {...register(`config.exits.${index}.priority` as const, {
                                setValueAs: toOptionalNumber
                              })}
                            />
                          </div>

                          <div className="grid gap-2">
                            <Label htmlFor={`exit-rule-min-hold-${index}`}>Min Hold Bars</Label>
                            <Input
                              id={`exit-rule-min-hold-${index}`}
                              type="number"
                              {...register(`config.exits.${index}.minHoldBars` as const, {
                                valueAsNumber: true
                              })}
                            />
                          </div>

                          <div className="flex items-end justify-between gap-3 rounded-xl border px-4 py-3">
                            <div>
                              <p className="text-sm font-medium">Enabled</p>
                              <p className="text-xs text-muted-foreground">Scope: position, action: exit_full</p>
                            </div>
                            <Switch
                              aria-label={`Toggle exit rule ${index + 1}`}
                              checked={Boolean(currentRule?.enabled)}
                              onCheckedChange={(checked) =>
                                setValue(`config.exits.${index}.enabled` as const, Boolean(checked), {
                                  shouldDirty: true,
                                  shouldTouch: true
                                })
                              }
                            />
                          </div>
                        </div>

                        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
                          <div className="grid gap-2">
                            <Label htmlFor={`exit-rule-price-field-${index}`}>Price Field</Label>
                            <Select
                              value={currentRule?.priceField || (ruleType === 'take_profit_fixed' ? 'high' : ruleType === 'time_stop' ? 'close' : 'low')}
                              onValueChange={(value) =>
                                setValue(`config.exits.${index}.priceField` as const, value as ExitRulePriceField, {
                                  shouldDirty: true,
                                  shouldTouch: true
                                })
                              }
                            >
                              <SelectTrigger id={`exit-rule-price-field-${index}`}>
                                <SelectValue placeholder="Select price field" />
                              </SelectTrigger>
                              <SelectContent>
                                {PRICE_FIELD_OPTIONS.map((option) => (
                                  <SelectItem key={option.value} value={option.value}>
                                    {option.label}
                                  </SelectItem>
                                ))}
                              </SelectContent>
                            </Select>
                          </div>

                          <div className="grid gap-2">
                            <Label htmlFor={`exit-rule-value-${index}`}>{ruleValueLabel}</Label>
                            <Input
                              id={`exit-rule-value-${index}`}
                              type="number"
                              step={ruleType === 'time_stop' ? 1 : 0.01}
                              {...register(`config.exits.${index}.value` as const, {
                                setValueAs: toOptionalNumber
                              })}
                            />
                          </div>

                          {ruleType === 'trailing_stop_atr' ? (
                            <div className="grid gap-2 md:col-span-2">
                              <Label htmlFor={`exit-rule-atr-column-${index}`}>ATR Column</Label>
                              <Input
                                id={`exit-rule-atr-column-${index}`}
                                {...register(`config.exits.${index}.atrColumn` as const)}
                              />
                            </div>
                          ) : (
                            <div className="grid gap-2 md:col-span-2">
                              <Label htmlFor={`exit-rule-reference-${index}`}>Reference</Label>
                              <Input
                                id={`exit-rule-reference-${index}`}
                                readOnly
                                value={currentRule?.reference || (ruleType === 'take_profit_fixed' || ruleType === 'stop_loss_fixed' ? 'entry_price' : ruleType === 'time_stop' ? '' : 'highest_since_entry')}
                              />
                            </div>
                          )}
                        </div>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>

            <SheetFooter>
              <Button type="submit" disabled={mutation.isPending}>
                {mutation.isPending ? 'Saving...' : 'Save Strategy'}
              </Button>
            </SheetFooter>
          </form>
        )}
      </SheetContent>
    </Sheet>
  );
}
