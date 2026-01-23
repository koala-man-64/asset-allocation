// Alert Management Page - Configure and manage trading alerts

import { Card, CardContent, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Badge } from '@/app/components/ui/badge';
import { Button } from '@/app/components/ui/button';
import { 
  Bell, 
  Plus,
  Edit,
  Trash2,
  Mail,
  Smartphone,
  MessageSquare,
  TrendingDown,
  TrendingUp,
  AlertTriangle,
  Activity,
  DollarSign,
  BarChart3,
  Clock,
  CheckCircle2,
  XCircle
} from 'lucide-react';
import { useState } from 'react';
import { Switch } from '@/app/components/ui/switch';
import { InfoTooltip } from '@/app/components/ui/metric-tooltip';
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/app/components/ui/tooltip';

// Mock alert configurations
const mockAlerts = [
  {
    id: 'alert-001',
    name: 'Portfolio Daily Loss Limit',
    type: 'pnl',
    condition: 'Daily P&L < -$50,000',
    enabled: true,
    channels: ['email', 'sms', 'app'],
    priority: 'high',
    strategy: 'All Strategies',
    createdAt: '2025-01-15',
    triggeredCount: 3,
    lastTriggered: '2025-01-18 14:23:12'
  },
  {
    id: 'alert-002',
    name: 'Momentum Alpha Strong Signal',
    type: 'signal',
    condition: 'Signal Strength > 8.5',
    enabled: true,
    channels: ['app'],
    priority: 'medium',
    strategy: 'Momentum Alpha',
    createdAt: '2025-01-10',
    triggeredCount: 127,
    lastTriggered: '2025-01-20 09:15:43'
  },
  {
    id: 'alert-003',
    name: 'Margin Utilization Warning',
    type: 'risk',
    condition: 'Margin Used > 85%',
    enabled: true,
    channels: ['email', 'app'],
    priority: 'high',
    strategy: 'All Strategies',
    createdAt: '2025-01-08',
    triggeredCount: 8,
    lastTriggered: '2025-01-20 10:45:22'
  },
  {
    id: 'alert-004',
    name: 'Large Position Movement',
    type: 'position',
    condition: 'Position Change > 1000 shares',
    enabled: true,
    channels: ['email'],
    priority: 'medium',
    strategy: 'All Strategies',
    createdAt: '2025-01-12',
    triggeredCount: 45,
    lastTriggered: '2025-01-19 16:32:11'
  },
  {
    id: 'alert-005',
    name: 'Drawdown Threshold',
    type: 'risk',
    condition: 'Drawdown > 15%',
    enabled: false,
    channels: ['email', 'sms'],
    priority: 'critical',
    strategy: 'Value Mean Rev',
    createdAt: '2025-01-05',
    triggeredCount: 1,
    lastTriggered: '2025-01-14 11:20:33'
  },
  {
    id: 'alert-006',
    name: 'Tech Sector Daily Gain',
    type: 'pnl',
    condition: 'Daily P&L > +$25,000',
    enabled: true,
    channels: ['app'],
    priority: 'low',
    strategy: 'Tech Sector',
    createdAt: '2025-01-18',
    triggeredCount: 12,
    lastTriggered: '2025-01-20 15:47:29'
  },
  {
    id: 'alert-007',
    name: 'Order Fill Confirmation',
    type: 'execution',
    condition: 'Order Filled',
    enabled: true,
    channels: ['app', 'sms'],
    priority: 'medium',
    strategy: 'All Strategies',
    createdAt: '2025-01-01',
    triggeredCount: 342,
    lastTriggered: '2025-01-20 09:28:43'
  },
  {
    id: 'alert-008',
    name: 'Volatility Spike Detection',
    type: 'market',
    condition: 'VIX > 30',
    enabled: true,
    channels: ['email', 'app'],
    priority: 'high',
    strategy: 'All Strategies',
    createdAt: '2025-01-03',
    triggeredCount: 5,
    lastTriggered: '2025-01-17 13:12:55'
  }
];

// Mock alert history/logs
const mockAlertHistory = [
  { id: 'log-001', alertName: 'Portfolio Daily Loss Limit', triggeredAt: '2025-01-20 10:45:22', message: 'Daily P&L reached -$52,340 (threshold: -$50,000)', acknowledged: true },
  { id: 'log-002', alertName: 'Momentum Alpha Strong Signal', triggeredAt: '2025-01-20 09:15:43', message: 'Signal strength 8.7 detected for AAPL', acknowledged: true },
  { id: 'log-003', alertName: 'Order Fill Confirmation', triggeredAt: '2025-01-20 09:28:43', message: 'MSFT SELL 50 shares filled @ $418.92', acknowledged: true },
  { id: 'log-004', alertName: 'Margin Utilization Warning', triggeredAt: '2025-01-20 10:45:22', message: 'Margin utilization at 87%', acknowledged: false },
  { id: 'log-005', alertName: 'Tech Sector Daily Gain', triggeredAt: '2025-01-20 15:47:29', message: 'Daily P&L reached +$27,450', acknowledged: true },
  { id: 'log-006', alertName: 'Large Position Movement', triggeredAt: '2025-01-19 16:32:11', message: 'AAPL position increased by 1,200 shares', acknowledged: true },
  { id: 'log-007', alertName: 'Volatility Spike Detection', triggeredAt: '2025-01-17 13:12:55', message: 'VIX reached 32.4', acknowledged: true },
];

const alertTypeConfig = {
  pnl: { icon: DollarSign, label: 'P&L Alert', color: 'text-green-600' },
  signal: { icon: Activity, label: 'Signal Alert', color: 'text-blue-600' },
  risk: { icon: AlertTriangle, label: 'Risk Alert', color: 'text-orange-600' },
  position: { icon: BarChart3, label: 'Position Alert', color: 'text-purple-600' },
  execution: { icon: CheckCircle2, label: 'Execution Alert', color: 'text-teal-600' },
  market: { icon: TrendingUp, label: 'Market Alert', color: 'text-indigo-600' }
};

export function AlertsPage() {
  const [alerts, setAlerts] = useState(mockAlerts);
  const [selectedTab, setSelectedTab] = useState<'active' | 'history'>('active');

  const toggleAlert = (alertId: string) => {
    setAlerts(alerts.map(alert => 
      alert.id === alertId ? { ...alert, enabled: !alert.enabled } : alert
    ));
  };

  const getPriorityBadge = (priority: string) => {
    switch (priority) {
      case 'critical':
        return <Badge className="bg-red-100 text-red-800 border-red-200">Critical</Badge>;
      case 'high':
        return <Badge className="bg-orange-100 text-orange-800 border-orange-200">High</Badge>;
      case 'medium':
        return <Badge className="bg-yellow-100 text-yellow-800 border-yellow-200">Medium</Badge>;
      case 'low':
        return <Badge className="bg-blue-100 text-blue-800 border-blue-200">Low</Badge>;
      default:
        return <Badge variant="outline">{priority}</Badge>;
    }
  };

  const getChannelIcon = (channel: string) => {
    switch (channel) {
      case 'email':
        return <Mail className="h-3.5 w-3.5" />;
      case 'sms':
        return <Smartphone className="h-3.5 w-3.5" />;
      case 'app':
        return <Bell className="h-3.5 w-3.5" />;
      case 'slack':
        return <MessageSquare className="h-3.5 w-3.5" />;
      default:
        return null;
    }
  };

  const activeAlerts = alerts.filter(a => a.enabled);
  const inactiveAlerts = alerts.filter(a => !a.enabled);

  return (
    <div className="space-y-6">
      {/* Header */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle className="flex items-center gap-2">
                <Bell className="h-6 w-6" />
                Alert Management
              </CardTitle>
              <p className="text-sm text-muted-foreground mt-1">
                Configure and manage alerts for trading strategies, risk metrics, and market conditions
              </p>
            </div>
            <Button className="gap-2">
              <Plus className="h-4 w-4" />
              Create Alert
            </Button>
          </div>
        </CardHeader>
      </Card>

      {/* Summary Stats */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <Card>
          <CardContent className="pt-6">
            <div className="flex items-center justify-between">
              <div>
                <div className="text-sm text-muted-foreground mb-1">Active Alerts</div>
                <div className="text-2xl font-bold">{activeAlerts.length}</div>
              </div>
              <CheckCircle2 className="h-8 w-8 text-green-600" />
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="pt-6">
            <div className="flex items-center justify-between">
              <div>
                <div className="text-sm text-muted-foreground mb-1">Inactive Alerts</div>
                <div className="text-2xl font-bold">{inactiveAlerts.length}</div>
              </div>
              <XCircle className="h-8 w-8 text-muted-foreground" />
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="pt-6">
            <div className="flex items-center justify-between">
              <div>
                <div className="text-sm text-muted-foreground mb-1">Triggered Today</div>
                <div className="text-2xl font-bold">24</div>
              </div>
              <Activity className="h-8 w-8 text-blue-600" />
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="pt-6">
            <div className="flex items-center justify-between">
              <div>
                <div className="text-sm text-muted-foreground mb-1">Pending Action</div>
                <div className="text-2xl font-bold">1</div>
              </div>
              <AlertTriangle className="h-8 w-8 text-orange-600" />
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Tabs */}
      <div className="flex gap-2 border-b">
        <Button
          variant={selectedTab === 'active' ? 'default' : 'ghost'}
          size="sm"
          onClick={() => setSelectedTab('active')}
          className="rounded-b-none"
        >
          Active Alerts
        </Button>
        <Button
          variant={selectedTab === 'history' ? 'default' : 'ghost'}
          size="sm"
          onClick={() => setSelectedTab('history')}
          className="rounded-b-none"
        >
          Alert History
        </Button>
      </div>

      {/* Active Alerts Table */}
      {selectedTab === 'active' && (
        <Card>
          <CardHeader>
            <CardTitle>Configured Alerts</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-3">
              {alerts.map(alert => {
                const typeConfig = alertTypeConfig[alert.type as keyof typeof alertTypeConfig];
                const TypeIcon = typeConfig.icon;

                return (
                  <div
                    key={alert.id}
                    className={`border rounded-lg p-4 transition-all ${
                      alert.enabled ? 'bg-white' : 'bg-muted/30 opacity-60'
                    }`}
                  >
                    <div className="flex items-start justify-between gap-4">
                      <div className="flex items-start gap-4 flex-1">
                        {/* Icon */}
                        <div className={`p-2 rounded-lg bg-${alert.enabled ? 'primary' : 'muted'}/10 mt-1`}>
                          <TypeIcon className={`h-5 w-5 ${alert.enabled ? typeConfig.color : 'text-muted-foreground'}`} />
                        </div>

                        {/* Alert Details */}
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-2 mb-1">
                            <h3 className="font-semibold">{alert.name}</h3>
                            {getPriorityBadge(alert.priority)}
                            <Badge variant="outline" className="text-xs">
                              {typeConfig.label}
                            </Badge>
                          </div>
                          
                          <div className="text-sm text-muted-foreground space-y-1">
                            <div className="flex items-center gap-2">
                              <span className="font-mono font-semibold text-foreground">{alert.condition}</span>
                              <span>•</span>
                              <span>{alert.strategy}</span>
                            </div>
                            <div className="flex items-center gap-3 text-xs">
                              <div className="flex items-center gap-1">
                                <Clock className="h-3 w-3" />
                                Created: {alert.createdAt}
                              </div>
                              {alert.lastTriggered && (
                                <>
                                  <span>•</span>
                                  <div>Last triggered: {alert.lastTriggered}</div>
                                </>
                              )}
                              <span>•</span>
                              <div>Triggered {alert.triggeredCount}x</div>
                            </div>
                          </div>

                          {/* Channels */}
                          <div className="flex items-center gap-2 mt-2">
                            <span className="text-xs text-muted-foreground">Channels:</span>
                            <div className="flex gap-1">
                              {alert.channels.map(channel => (
                                <Badge 
                                  key={channel} 
                                  variant="secondary" 
                                  className="flex items-center gap-1 text-xs px-2 py-0.5"
                                >
                                  {getChannelIcon(channel)}
                                  {channel}
                                </Badge>
                              ))}
                            </div>
                          </div>
                        </div>
                      </div>

                      {/* Actions */}
                      <div className="flex items-center gap-2">
                        <Switch
                          checked={alert.enabled}
                          onCheckedChange={() => toggleAlert(alert.id)}
                        />
                        <Button variant="ghost" size="sm" className="h-8 w-8 p-0">
                          <Edit className="h-4 w-4" />
                        </Button>
                        <Button variant="ghost" size="sm" className="h-8 w-8 p-0 text-destructive">
                          <Trash2 className="h-4 w-4" />
                        </Button>
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Alert History */}
      {selectedTab === 'history' && (
        <Card>
          <CardHeader>
            <CardTitle>Recent Alert History</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b">
                    <th className="text-left p-3">Alert Name</th>
                    <th className="text-left p-3">Triggered At</th>
                    <th className="text-left p-3">Message</th>
                    <th className="text-left p-3">Status</th>
                    <th className="text-left p-3">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {mockAlertHistory.map(log => (
                    <tr key={log.id} className="border-b hover:bg-muted/50">
                      <td className="p-3 font-semibold">{log.alertName}</td>
                      <td className="p-3 font-mono text-xs">{log.triggeredAt}</td>
                      <td className="p-3 text-muted-foreground">{log.message}</td>
                      <td className="p-3">
                        {log.acknowledged ? (
                          <Badge className="bg-green-100 text-green-800 border-green-200">
                            <CheckCircle2 className="h-3 w-3 mr-1" />
                            Acknowledged
                          </Badge>
                        ) : (
                          <Badge className="bg-yellow-100 text-yellow-800 border-yellow-200">
                            <AlertTriangle className="h-3 w-3 mr-1" />
                            Pending
                          </Badge>
                        )}
                      </td>
                      <td className="p-3">
                        {!log.acknowledged && (
                          <Button variant="outline" size="sm" className="h-7 text-xs">
                            Acknowledge
                          </Button>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Alert Configuration Templates */}
      <Card>
        <CardHeader>
          <div className="flex items-center gap-2">
            <CardTitle>Quick Alert Templates</CardTitle>
            <TooltipProvider>
              <Tooltip>
                <TooltipTrigger>
                  <InfoTooltip 
                    content={
                      <div className="space-y-2">
                        <p className="font-semibold">Alert Templates</p>
                        <p className="text-xs">Pre-configured alerts for common trading scenarios. Click to create a new alert with default settings that you can customize.</p>
                      </div>
                    }
                  />
                </TooltipTrigger>
              </Tooltip>
            </TooltipProvider>
          </div>
          <p className="text-sm text-muted-foreground mt-1">
            Pre-configured alert templates for common scenarios
          </p>
        </CardHeader>
        <CardContent>
          <TooltipProvider>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <Tooltip>
                <TooltipTrigger asChild>
                  <div className="border rounded-lg p-4 hover:border-primary/50 transition-colors cursor-pointer">
                    <div className="flex items-center gap-3 mb-2">
                      <div className="p-2 rounded-lg bg-red-100">
                        <TrendingDown className="h-5 w-5 text-red-600" />
                      </div>
                      <h4 className="font-semibold">Daily Loss Limit</h4>
                    </div>
                    <p className="text-sm text-muted-foreground mb-3">
                      Alert when daily P&L falls below a threshold
                    </p>
                    <Button variant="outline" size="sm" className="w-full">
                      <Plus className="h-3 w-3 mr-1" />
                      Create from Template
                    </Button>
                  </div>
                </TooltipTrigger>
                <TooltipContent side="top" className="max-w-xs">
                  <p className="font-semibold text-sm">Daily Loss Limit Alert</p>
                  <p className="text-xs mt-1">Get notified when your portfolio loses more than a specified amount in a single day. Critical for risk management and protecting capital.</p>
                </TooltipContent>
              </Tooltip>

              <Tooltip>
                <TooltipTrigger asChild>
                  <div className="border rounded-lg p-4 hover:border-primary/50 transition-colors cursor-pointer">
                    <div className="flex items-center gap-3 mb-2">
                      <div className="p-2 rounded-lg bg-orange-100">
                        <AlertTriangle className="h-5 w-5 text-orange-600" />
                      </div>
                      <h4 className="font-semibold">Risk Threshold</h4>
                    </div>
                    <p className="text-sm text-muted-foreground mb-3">
                      Alert on margin, drawdown, or VaR breaches
                    </p>
                    <Button variant="outline" size="sm" className="w-full">
                      <Plus className="h-3 w-3 mr-1" />
                      Create from Template
                    </Button>
                  </div>
                </TooltipTrigger>
                <TooltipContent side="top" className="max-w-xs">
                  <p className="font-semibold text-sm">Risk Threshold Alert</p>
                  <p className="text-xs mt-1">Monitor risk metrics like margin utilization, drawdown percentage, or Value at Risk (VaR). Helps prevent over-leveraging and excessive losses.</p>
                </TooltipContent>
              </Tooltip>

              <Tooltip>
                <TooltipTrigger asChild>
                  <div className="border rounded-lg p-4 hover:border-primary/50 transition-colors cursor-pointer">
                    <div className="flex items-center gap-3 mb-2">
                      <div className="p-2 rounded-lg bg-blue-100">
                        <Activity className="h-5 w-5 text-blue-600" />
                      </div>
                      <h4 className="font-semibold">Strategy Signal</h4>
                    </div>
                    <p className="text-sm text-muted-foreground mb-3">
                      Alert on strong buy/sell signals from strategies
                    </p>
                    <Button variant="outline" size="sm" className="w-full">
                      <Plus className="h-3 w-3 mr-1" />
                      Create from Template
                    </Button>
                  </div>
                </TooltipTrigger>
                <TooltipContent side="top" className="max-w-xs">
                  <p className="font-semibold text-sm">Strategy Signal Alert</p>
                  <p className="text-xs mt-1">Get notified when your trading strategies generate high-confidence buy or sell signals. Useful for semi-automated trading workflows.</p>
                </TooltipContent>
              </Tooltip>
            </div>
          </TooltipProvider>
        </CardContent>
      </Card>

      {/* Notification Settings */}
      <Card>
        <CardHeader>
          <CardTitle>Notification Channels</CardTitle>
          <p className="text-sm text-muted-foreground mt-1">
            Configure how you receive alert notifications
          </p>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            <div className="flex items-center justify-between p-4 border rounded-lg">
              <div className="flex items-center gap-3">
                <Mail className="h-5 w-5 text-muted-foreground" />
                <div>
                  <div className="font-semibold">Email Notifications</div>
                  <div className="text-sm text-muted-foreground">trader@quantcore.com</div>
                </div>
              </div>
              <div className="flex items-center gap-3">
                <Badge className="bg-green-100 text-green-800 border-green-200">Connected</Badge>
                <Button variant="outline" size="sm">Configure</Button>
              </div>
            </div>

            <div className="flex items-center justify-between p-4 border rounded-lg">
              <div className="flex items-center gap-3">
                <Smartphone className="h-5 w-5 text-muted-foreground" />
                <div>
                  <div className="font-semibold">SMS Notifications</div>
                  <div className="text-sm text-muted-foreground">+1 (555) 123-4567</div>
                </div>
              </div>
              <div className="flex items-center gap-3">
                <Badge className="bg-green-100 text-green-800 border-green-200">Connected</Badge>
                <Button variant="outline" size="sm">Configure</Button>
              </div>
            </div>

            <div className="flex items-center justify-between p-4 border rounded-lg">
              <div className="flex items-center gap-3">
                <MessageSquare className="h-5 w-5 text-muted-foreground" />
                <div>
                  <div className="font-semibold">Slack Integration</div>
                  <div className="text-sm text-muted-foreground">#trading-alerts</div>
                </div>
              </div>
              <div className="flex items-center gap-3">
                <Badge variant="outline">Not Connected</Badge>
                <Button variant="outline" size="sm">Connect</Button>
              </div>
            </div>

            <div className="flex items-center justify-between p-4 border rounded-lg">
              <div className="flex items-center gap-3">
                <Bell className="h-5 w-5 text-muted-foreground" />
                <div>
                  <div className="font-semibold">In-App Notifications</div>
                  <div className="text-sm text-muted-foreground">Browser and desktop notifications</div>
                </div>
              </div>
              <div className="flex items-center gap-3">
                <Badge className="bg-green-100 text-green-800 border-green-200">Enabled</Badge>
                <Button variant="outline" size="sm">Configure</Button>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}