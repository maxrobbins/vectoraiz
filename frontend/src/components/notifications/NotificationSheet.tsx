import { useEffect, useState } from "react";
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { Button } from "@/components/ui/button";
import {
  CheckCircle2,
  AlertTriangle,
  AlertCircle,
  Info,
  Bell,
} from "lucide-react";
import { notificationsApi, type ApiNotification } from "@/lib/api";

const typeConfig: Record<string, { icon: typeof Info; color: string }> = {
  success: { icon: CheckCircle2, color: "text-green-500" },
  error: { icon: AlertCircle, color: "text-red-500" },
  warning: { icon: AlertTriangle, color: "text-amber-500" },
  info: { icon: Info, color: "text-blue-500" },
  action_required: { icon: Bell, color: "text-purple-500" },
};

function timeAgo(dateStr: string): string {
  const diff = Date.now() - new Date(dateStr).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

interface NotificationSheetProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onUnreadCountChange?: (count: number) => void;
}

export default function NotificationSheet({
  open,
  onOpenChange,
  onUnreadCountChange,
}: NotificationSheetProps) {
  const [notifications, setNotifications] = useState<ApiNotification[]>([]);
  const [loading, setLoading] = useState(false);

  const fetchNotifications = async () => {
    setLoading(true);
    try {
      const res = await notificationsApi.list({ limit: 50 });
      setNotifications(res.notifications);
    } catch {
      // silent
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (open) fetchNotifications();
  }, [open]);

  const handleMarkRead = async (id: string) => {
    try {
      await notificationsApi.markRead(id);
      setNotifications((prev) =>
        prev.map((n) => (n.id === id ? { ...n, read: true } : n))
      );
      onUnreadCountChange?.(notifications.filter((n) => !n.read && n.id !== id).length);
    } catch {
      // silent
    }
  };

  const handleMarkAllRead = async () => {
    try {
      await notificationsApi.markAllRead();
      setNotifications((prev) => prev.map((n) => ({ ...n, read: true })));
      onUnreadCountChange?.(0);
    } catch {
      // silent
    }
  };

  const unreadCount = notifications.filter((n) => !n.read).length;

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent side="right" className="w-[400px] sm:w-[440px] flex flex-col">
        <SheetHeader className="flex flex-row items-center justify-between pr-6">
          <SheetTitle>Notifications</SheetTitle>
          {unreadCount > 0 && (
            <Button
              variant="ghost"
              size="sm"
              className="text-xs text-muted-foreground"
              onClick={handleMarkAllRead}
            >
              Mark all read
            </Button>
          )}
        </SheetHeader>

        <div className="flex-1 overflow-y-auto mt-4 -mx-6 px-6">
          {loading && notifications.length === 0 && (
            <div className="text-sm text-muted-foreground text-center py-8">
              Loading...
            </div>
          )}

          {!loading && notifications.length === 0 && (
            <div className="text-sm text-muted-foreground text-center py-8">
              No notifications yet
            </div>
          )}

          <div className="space-y-1">
            {notifications.map((n) => {
              const cfg = typeConfig[n.type] || typeConfig.info;
              const Icon = cfg.icon;
              return (
                <button
                  key={n.id}
                  className={`w-full text-left p-3 rounded-lg transition-colors cursor-pointer ${
                    n.read
                      ? "opacity-60 hover:bg-secondary/50"
                      : "bg-secondary/50 hover:bg-secondary"
                  }`}
                  onClick={() => !n.read && handleMarkRead(n.id)}
                >
                  <div className="flex gap-3">
                    <Icon className={`w-4 h-4 mt-0.5 shrink-0 ${cfg.color}`} />
                    <div className="min-w-0 flex-1">
                      <div className="flex items-center gap-2">
                        <span className="text-sm font-medium truncate">
                          {n.title}
                        </span>
                        {!n.read && (
                          <span className="w-2 h-2 rounded-full bg-blue-500 shrink-0" />
                        )}
                      </div>
                      <p className="text-xs text-muted-foreground mt-0.5 line-clamp-2">
                        {n.message}
                      </p>
                      <span className="text-[10px] text-muted-foreground mt-1 block">
                        {timeAgo(n.created_at)}
                      </span>
                    </div>
                  </div>
                </button>
              );
            })}
          </div>
        </div>
      </SheetContent>
    </Sheet>
  );
}
