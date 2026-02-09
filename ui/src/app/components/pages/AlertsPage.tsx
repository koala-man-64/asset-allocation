import { Card, CardContent, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Bell } from 'lucide-react';

export function AlertsPage() {
  return (
    <div className="page-shell">
      <div className="page-header">
        <p className="page-kicker">Live Operations</p>
        <h1 className="page-title">Alerts</h1>
        <p className="page-subtitle">
          Operational alert routing, acknowledgement, and escalation controls.
        </p>
      </div>
      <Card className="mcm-panel">
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Bell className="h-5 w-5" />
            Alert Configuration
          </CardTitle>
        </CardHeader>
        <CardContent className="text-sm text-muted-foreground">
          Trading alert configuration is not implemented yet. Operational alerts
          (ack/snooze/resolve) are available on the System Status page.
        </CardContent>
      </Card>
    </div>
  );
}
