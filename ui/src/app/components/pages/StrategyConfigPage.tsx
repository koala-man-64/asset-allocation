import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Plus } from 'lucide-react';
import { Strategy, strategyApi } from '@/services/strategyApi';
import { DataTable } from '@/app/components/common/DataTable';
import { StrategyEditor } from '@/app/components/pages/StrategyEditor';
import { Button } from '@/app/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/app/components/ui/card';

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
        <div className="space-y-6">
            <div className="flex justify-between items-center">
                <div>
                    <h1 className="text-3xl font-bold tracking-tight">Strategies</h1>
                    <p className="text-muted-foreground">
                        Manage trading strategies and configurations.
                    </p>
                </div>
                <Button onClick={handleCreate}>
                    <Plus className="mr-2 h-4 w-4" /> New Strategy
                </Button>
            </div>

            <Card>
                <CardHeader>
                    <CardTitle>All Strategies</CardTitle>
                </CardHeader>
                <CardContent>
                    {isLoading ? (
                        <div className="text-center py-4">Loading strategies...</div>
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
