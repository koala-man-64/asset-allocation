import { Card, CardContent, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Database } from 'lucide-react';

export function DataTiersPage() {
  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Database className="h-5 w-5" />
            Data Tiers
          </CardTitle>
        </CardHeader>
        <CardContent className="text-sm text-muted-foreground">
          This page is not configured in this deployment. Use System Status for data freshness, pipeline links, and
          lineage/impact details.
        </CardContent>
      </Card>
    </div>
  );
}

