import { useEffect } from 'react';
import { useForm, Path } from 'react-hook-form';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Button } from '@/app/components/ui/button';
import { Input } from '@/app/components/ui/input';
import { Label } from '@/app/components/ui/label';
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
import { Strategy, strategyApi } from '@/services/strategyApi';
import { toast } from 'sonner';
import { formatSystemStatusText } from '@/utils/formatSystemStatusText';

interface StrategyEditorProps {
  strategy: Strategy | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function StrategyEditor({ strategy, open, onOpenChange }: StrategyEditorProps) {
  const queryClient = useQueryClient();
  const isEditing = !!strategy?.name;

  const {
    register,
    handleSubmit,
    reset,
    setValue,
    watch,
    formState: { errors }
  } = useForm<Strategy>({
    defaultValues: {
      name: '',
      type: 'configured',
      description: '',
      config: {
        universe: 'SP500',
        rebalance: 'monthly',
        lookbackWindow: 63,
        holdingPeriod: 21
      }
    }
  });

  useEffect(() => {
    if (open && strategy) {
      reset(strategy);
    } else if (open && !strategy) {
      reset({
        name: '',
        type: 'configured',
        description: '',
        config: {
          universe: 'SP500',
          rebalance: 'monthly',
          lookbackWindow: 63,
          holdingPeriod: 21,
          costModel: 'default'
        }
      });
    }
  }, [open, strategy, reset]);

  const mutation = useMutation({
    mutationFn: (data: Strategy) => strategyApi.saveStrategy(data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['strategies'] });
      toast.success(`Strategy ${isEditing ? 'updated' : 'created'} successfully`);
      onOpenChange(false);
    },
    onError: (error) => {
      toast.error(`Failed to save strategy: ${formatSystemStatusText(error)}`);
    }
  });

  const onSubmit = (data: Strategy) => {
    mutation.mutate(data);
  };

  // Helper for Select components since they don't integrate directly with register
  const handleSelectChange = (key: string, value: string) => {
    setValue(key as Path<Strategy>, value);
  };

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="overflow-y-auto sm:max-w-md">
        <SheetHeader>
          <SheetTitle>{isEditing ? 'Edit Strategy' : 'New Strategy'}</SheetTitle>
          <SheetDescription>Configure strategy parameters and metadata.</SheetDescription>
        </SheetHeader>

        <form onSubmit={handleSubmit(onSubmit)} className="space-y-6 py-4">
          <div className="space-y-4">
            <h3 className="text-sm font-medium text-muted-foreground">Metadata</h3>
            <div className="grid gap-2">
              <Label htmlFor="name">Name</Label>
              <Input
                id="name"
                disabled={isEditing}
                {...register('name', { required: true })}
                placeholder="e.g. mom-spy-res"
              />
              {errors.name && <span className="text-xs text-red-500">Name is required</span>}
            </div>

            <div className="grid gap-2">
              <Label htmlFor="description">Description</Label>
              <Input id="description" {...register('description')} />
            </div>

            <div className="grid gap-2">
              <Label htmlFor="type">Type</Label>
              <Select
                onValueChange={(val) => handleSelectChange('type', val)}
                defaultValue={watch('type')}
              >
                <SelectTrigger>
                  <SelectValue placeholder="Select type" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="configured">Configured</SelectItem>
                  <SelectItem value="code-based">Code Based</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>

          <div className="space-y-4 border-t pt-4">
            <h3 className="text-sm font-medium text-muted-foreground">Configuration</h3>

            <div className="grid gap-2">
              <Label htmlFor="universe">Universe</Label>
              <Input
                id="universe"
                {...register('config.universe')}
                placeholder="SP500, NDX, etc."
              />
            </div>

            <div className="grid gap-2">
              <Label htmlFor="rebalance">Rebalance Frequency</Label>
              <Select
                onValueChange={(val) => handleSelectChange('config.rebalance', val)}
                defaultValue={watch('config.rebalance')}
              >
                <SelectTrigger>
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

            <div className="grid grid-cols-2 gap-4">
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
            </div>
          </div>

          <SheetFooter>
            <Button type="submit" disabled={mutation.isPending}>
              {mutation.isPending ? 'Saving...' : 'Save Strategy'}
            </Button>
          </SheetFooter>
        </form>
      </SheetContent>
    </Sheet>
  );
}
