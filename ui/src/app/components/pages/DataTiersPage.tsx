import { Card, CardContent, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Database } from 'lucide-react';

export function DataTiersPage() {
  return (
    <div className="page-shell">
      <div className="page-header">
        <p className="page-kicker">Data Platform</p>
        <h1 className="page-title">Data Tiers</h1>
        <p className="page-subtitle">
          Canonical medallion-layer inventory and freshness posture.
        </p>
      </div>
      <Card className="mcm-panel">
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Database className="h-5 w-5" />
            Tier Overview
          </CardTitle>
        </CardHeader>
        <CardContent className="text-sm text-muted-foreground">
          This page is not configured in this deployment. Use System Status for data freshness,
          pipeline links, and lineage/impact details.
        </CardContent>
      </Card>
    </div>
  );
}
