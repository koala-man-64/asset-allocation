import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Plus } from 'lucide-react';
import { Strategy, strategyApi } from '@/services/strategyApi';
import { DataTable } from '@/app/components/common/DataTable';
import { StrategyEditor } from '@/app/components/pages/StrategyEditor';
import { Button } from '@/app/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/app/components/ui/card';
import { PageLoader } from '@/app/components/common/PageLoader';

export function StrategyConfigPage() {
  const [isEditorOpen, setIsEditorOpen] = useState(false);
  const [selectedStrategy, setSelectedStrategy] = useState<Strategy | null>(null);

  const { data: strategies = [], isLoading } = useQuery({
    queryKey: ['strategies'],
    queryFn: () => strategyApi.listStrategies()
  });

  const handleCreate = () => {
    setSelectedStrategy(null);
    setIsEditorOpen(true);
  };

  const handleRowClick = (strategy: Strategy) => {
    setSelectedStrategy(strategy);
    setIsEditorOpen(true);
  };

  const columns = [
    { header: 'Name', accessorKey: 'name' },
    { header: 'Type', accessorKey: 'type' },
    { header: 'Description', accessorKey: 'description' },
    { header: 'Last Updated', accessorKey: 'updated_at' }
  ];

  return (
    <div className="page-shell">
      <div className="page-header-row">
        <div className="page-header">
          <p className="page-kicker">Strategy Workbench</p>
          <h1 className="page-title">Strategies</h1>
          <p className="page-subtitle">Manage trading strategies and execution configurations.</p>
        </div>
        <Button onClick={handleCreate} className="gap-2">
          <Plus className="mr-2 h-4 w-4" /> New Strategy
        </Button>
      </div>

      <Card className="mcm-panel">
        <CardHeader>
          <CardTitle>All Strategies</CardTitle>
        </CardHeader>
        <CardContent>
          {isLoading ? (
            <PageLoader text="Loading strategies..." className="h-64" />
          ) : (
            <DataTable
              data={strategies}
              columns={columns}
              onRowClick={handleRowClick}
              emptyMessage="No strategies found. Create one to get started."
            />
          )}
        </CardContent>
      </Card>

      <StrategyEditor
        strategy={selectedStrategy}
        open={isEditorOpen}
        onOpenChange={setIsEditorOpen}
      />
    </div>
  );
}
