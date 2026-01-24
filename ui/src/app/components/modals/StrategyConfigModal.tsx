import type { StrategyRun } from '@/types/strategy';
import { Card, CardContent, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Badge } from '@/app/components/ui/badge';
import { Button } from '@/app/components/ui/button';
import { Table, TableBody, TableCell, TableRow } from '@/app/components/ui/table';
import { FileCode, Settings, X } from 'lucide-react';

interface StrategyConfigModalProps {
  strategy: StrategyRun | null;
  open: boolean;
  onClose: () => void;
}

export function StrategyConfigModal({ strategy, open, onClose }: StrategyConfigModalProps) {
  if (!open || !strategy) return null;

  const cfg = strategy.config;

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
      <div className="bg-background rounded-lg shadow-2xl max-w-4xl w-full max-h-[90vh] overflow-hidden flex flex-col">
        <div className="border-b px-6 py-4 flex items-center justify-between bg-gradient-to-r from-primary/5 to-purple-500/5">
          <div className="flex items-center gap-3">
            <div className="p-2 rounded-lg bg-primary/10">
              <Settings className="h-6 w-6 text-primary" />
            </div>
            <div>
              <h2 className="text-xl font-bold">{strategy.name}</h2>
              <p className="text-sm text-muted-foreground">Strategy configuration (as executed)</p>
            </div>
          </div>
          <Button variant="ghost" size="icon" onClick={onClose}>
            <X className="h-5 w-5" />
          </Button>
        </div>

        <div className="flex-1 overflow-y-auto p-6 space-y-6">
          <Card>
            <CardHeader>
              <div className="flex items-center gap-2">
                <FileCode className="h-5 w-5 text-muted-foreground" />
                <CardTitle>Strategy Metadata</CardTitle>
              </div>
            </CardHeader>
            <CardContent className="space-y-4">
              <Table>
                <TableBody>
                  <TableRow>
                    <TableCell className="text-muted-foreground">Run ID</TableCell>
                    <TableCell className="font-mono">{strategy.id}</TableCell>
                  </TableRow>
                  <TableRow>
                    <TableCell className="text-muted-foreground">Start</TableCell>
                    <TableCell className="font-mono">{strategy.startDate}</TableCell>
                  </TableRow>
                  <TableRow>
                    <TableCell className="text-muted-foreground">End</TableCell>
                    <TableCell className="font-mono">{strategy.endDate}</TableCell>
                  </TableRow>
                  <TableRow>
                    <TableCell className="text-muted-foreground">Git SHA</TableCell>
                    <TableCell className="font-mono">{strategy.audit.gitSha}</TableCell>
                  </TableRow>
                  <TableRow>
                    <TableCell className="text-muted-foreground">Data Version</TableCell>
                    <TableCell className="font-mono">{strategy.audit.dataVersionId}</TableCell>
                  </TableRow>
                  <TableRow>
                    <TableCell className="text-muted-foreground">Config Hash</TableCell>
                    <TableCell className="font-mono">{strategy.audit.configHash}</TableCell>
                  </TableRow>
                </TableBody>
              </Table>
              {strategy.tags?.length > 0 && (
                <div className="flex flex-wrap gap-2">
                  {strategy.tags.map((tag) => (
                    <Badge key={tag} variant="outline">
                      {tag}
                    </Badge>
                  ))}
                </div>
              )}
              {strategy.audit.warnings?.length > 0 && (
                <div className="text-sm text-muted-foreground">
                  Warnings: {strategy.audit.warnings.join(', ')}
                </div>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Config</CardTitle>
            </CardHeader>
            <CardContent>
              <Table>
                <TableBody>
                  <TableRow>
                    <TableCell className="text-muted-foreground">Universe</TableCell>
                    <TableCell>{cfg.universe}</TableCell>
                  </TableRow>
                  <TableRow>
                    <TableCell className="text-muted-foreground">Rebalance</TableCell>
                    <TableCell>{cfg.rebalance}</TableCell>
                  </TableRow>
                  <TableRow>
                    <TableCell className="text-muted-foreground">Position Type</TableCell>
                    <TableCell>{cfg.longOnly ? 'Long Only' : 'Long/Short'}</TableCell>
                  </TableRow>
                  <TableRow>
                    <TableCell className="text-muted-foreground">Top N</TableCell>
                    <TableCell>{cfg.topN}</TableCell>
                  </TableRow>
                  <TableRow>
                    <TableCell className="text-muted-foreground">Lookback</TableCell>
                    <TableCell>{cfg.lookbackWindow} days</TableCell>
                  </TableRow>
                  <TableRow>
                    <TableCell className="text-muted-foreground">Holding Period</TableCell>
                    <TableCell>{cfg.holdingPeriod} days</TableCell>
                  </TableRow>
                  <TableRow>
                    <TableCell className="text-muted-foreground">Cost Model</TableCell>
                    <TableCell>{cfg.costModel}</TableCell>
                  </TableRow>
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}

