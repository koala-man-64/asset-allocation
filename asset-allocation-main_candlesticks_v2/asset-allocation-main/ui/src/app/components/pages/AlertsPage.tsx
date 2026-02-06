import { Card, CardContent, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Bell } from 'lucide-react';

export function AlertsPage() {
  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Bell className="h-5 w-5" />
            Alerts
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
