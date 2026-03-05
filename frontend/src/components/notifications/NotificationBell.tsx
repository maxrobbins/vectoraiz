import { useEffect, useState, useCallback } from "react";
import { Bell } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { notificationsApi } from "@/lib/api";
import NotificationSheet from "./NotificationSheet";

const POLL_INTERVAL_MS = 30_000;

export default function NotificationBell() {
  const [unreadCount, setUnreadCount] = useState(0);
  const [sheetOpen, setSheetOpen] = useState(false);

  const fetchUnreadCount = useCallback(async () => {
    try {
      const res = await notificationsApi.unreadCount();
      setUnreadCount(res.count);
    } catch {
      // silent — backend may be unreachable
    }
  }, []);

  useEffect(() => {
    fetchUnreadCount();
    const interval = setInterval(fetchUnreadCount, POLL_INTERVAL_MS);
    return () => clearInterval(interval);
  }, [fetchUnreadCount]);

  // Refresh count when sheet closes
  const handleOpenChange = (open: boolean) => {
    setSheetOpen(open);
    if (!open) fetchUnreadCount();
  };

  return (
    <>
      <Tooltip>
        <TooltipTrigger asChild>
          <Button
            variant="ghost"
            size="icon"
            className="relative w-9 h-9"
            data-notification-bell
            onClick={() => setSheetOpen(true)}
          >
            <Bell className="w-4 h-4 text-muted-foreground" />
            {unreadCount > 0 && (
              <span className="absolute -top-0.5 -right-0.5 min-w-[18px] h-[18px] rounded-full bg-red-500 text-white text-[10px] font-bold flex items-center justify-center px-1">
                {unreadCount > 99 ? "99+" : unreadCount}
              </span>
            )}
          </Button>
        </TooltipTrigger>
        <TooltipContent>
          {unreadCount > 0
            ? `${unreadCount} unread notification${unreadCount === 1 ? "" : "s"}`
            : "Notifications"}
        </TooltipContent>
      </Tooltip>

      <NotificationSheet
        open={sheetOpen}
        onOpenChange={handleOpenChange}
        onUnreadCountChange={setUnreadCount}
      />
    </>
  );
}
