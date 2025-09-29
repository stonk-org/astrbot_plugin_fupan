import json
import os
from datetime import datetime, time
from typing import Optional

import exchange_calendars as xcals
from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, register
from astrbot.core.star.star_tools import StarTools


@register(
    "astrbot_plugin_fupan",
    "zanderzhng",
    "复盘打卡插件，用于帮助交易者进行每日复盘打卡。支持交易日判断、时间窗口控制、数据统计等功能。",
    "1.0.0",
)
class FuPanPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config

        # 初始化交易所日历 (使用中国A股日历)
        self.xcal = xcals.get_calendar("XSHG")  # 上海证券交易所日历

        # 获取插件数据目录 (使用 AstrBot 官方提供的持久化数据目录)
        self.data_dir = str(StarTools.get_data_dir("astrbot_plugin_fupan"))
        logger.info(f"复盘打卡插件已加载，数据目录: {self.data_dir}")

    def get_checkin_data_file(self, user_id: str, group_id: Optional[str] = None) -> str:
        """获取用户打卡数据文件路径"""
        if group_id:
            return os.path.join(self.data_dir, f"checkin_{user_id}_group_{group_id}.json")
        else:
            return os.path.join(self.data_dir, f"checkin_{user_id}_dm.json")

    def get_all_checkin_files(self) -> list:
        """获取所有用户的打卡数据文件"""
        try:
            files = []
            for file_name in os.listdir(self.data_dir):
                if file_name.startswith("checkin_") and file_name.endswith(".json"):
                    files.append(os.path.join(self.data_dir, file_name))
            return files
        except (OSError, IOError) as e:
            logger.error(f"读取数据文件列表时出错: {e}")
            return []

    def load_user_checkin_data(self, user_id: str, group_id: Optional[str] = None) -> dict:
        """加载用户打卡数据"""
        try:
            data_file = self.get_checkin_data_file(user_id, group_id)
            if os.path.exists(data_file):
                with open(data_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    # Ensure strike_count field exists for backward compatibility
                    if "strike_count" not in data:
                        data["strike_count"] = 0
                    return data
            return {"user_id": user_id, "nickname": "", "checkins": [], "total_count": 0, "strike_count": 0}
        except (OSError, IOError, json.JSONDecodeError) as e:
            logger.error(f"加载用户 {user_id} 数据时出错: {e}")
            return {"user_id": user_id, "nickname": "", "checkins": [], "total_count": 0, "strike_count": 0}

    def save_user_checkin_data(self, user_id: str, data: dict, group_id: Optional[str] = None):
        """保存用户打卡数据"""
        try:
            data_file = self.get_checkin_data_file(user_id, group_id)
            with open(data_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except (OSError, IOError) as e:
            logger.error(f"保存用户 {user_id} 数据时出错: {e}")

    def reset_group_data(self, group_id: str) -> int:
        """重置指定群组的所有用户数据"""
        try:
            all_files = self.get_all_checkin_files()
            reset_count = 0

            # 删除指定群组的所有用户数据文件
            for file_path in all_files:
                if f"_group_{group_id}.json" in file_path:
                    try:
                        os.remove(file_path)
                        reset_count += 1
                    except (OSError, IOError) as e:
                        logger.error(f"删除文件 {file_path} 时出错: {e}")

            return reset_count
        except Exception as e:
            logger.error(f"重置群组 {group_id} 数据时出错: {e}")
            return 0

    def reset_all_dm_data(self) -> int:
        """重置所有私聊用户数据"""
        try:
            all_files = self.get_all_checkin_files()
            reset_count = 0

            # 删除所有私聊用户数据文件
            for file_path in all_files:
                if file_path.endswith("_dm.json"):
                    try:
                        os.remove(file_path)
                        reset_count += 1
                    except (OSError, IOError) as e:
                        logger.error(f"删除文件 {file_path} 时出错: {e}")

            return reset_count
        except Exception as e:
            logger.error(f"重置私聊数据时出错: {e}")
            return 0

    def is_trading_day(self, date: datetime) -> bool:
        """判断是否为交易日"""
        try:
            return self.xcal.is_session(date.date())
        except Exception as e:
            logger.error(f"判断交易日时出错: {e}")
            return False

    def get_previous_trading_day(self, date: datetime) -> Optional[datetime]:
        """获取前一个交易日"""
        try:
            previous_sessions = self.xcal.previous_session(date.date())
            if previous_sessions:
                return datetime.combine(previous_sessions, time())
            return None
        except Exception as e:
            logger.error(f"获取前一个交易日时出错: {e}")
            return None

    def get_next_trading_day(self, date: datetime) -> Optional[datetime]:
        """获取下一个交易日"""
        try:
            next_sessions = self.xcal.next_session(date.date())
            if next_sessions:
                return datetime.combine(next_sessions, time())
            return None
        except Exception as e:
            logger.error(f"获取下一个交易日时出错: {e}")
            return None

    def get_time_window_for_context(self, user_id: str, group_id: Optional[str] = None) -> tuple[str, str]:
        """获取指定用户或群组的时间窗口配置"""
        # Check for group-specific configuration
        if group_id and "fupan_checkin_group_time_windows" in self.config:
            group_configs = self.config["fupan_checkin_group_time_windows"]
            if isinstance(group_configs, dict) and group_id in group_configs:
                group_config = group_configs[group_id]
                if isinstance(group_config, dict) and "start_time" in group_config and "end_time" in group_config:
                    return group_config["start_time"], group_config["end_time"]

        # Check for user-specific configuration
        if "fupan_checkin_user_time_windows" in self.config:
            user_configs = self.config["fupan_checkin_user_time_windows"]
            if isinstance(user_configs, dict) and user_id in user_configs:
                user_config = user_configs[user_id]
                if isinstance(user_config, dict) and "start_time" in user_config and "end_time" in user_config:
                    return user_config["start_time"], user_config["end_time"]

        # Fall back to global configuration
        start_time = self.config.get("fupan_checkin_start_time", "15:00")
        end_time = self.config.get("fupan_checkin_end_time", "09:00")
        return start_time, end_time

    def get_current_trading_status(
        self, user_id: str, group_id: Optional[str] = None, now: Optional[datetime] = None
    ) -> dict:
        """获取当前交易状态，支持 per-group/per-user 配置"""
        if now is None:
            now = datetime.now()
        today = now.date()

        # 获取用户或群组特定的时间窗口配置
        start_time_str, end_time_str = self.get_time_window_for_context(user_id, group_id)

        # 判断今天是否为交易日
        is_today_trading = self.is_trading_day(now)

        if is_today_trading:
            # 获取下一个交易日
            next_trading_day = self.get_next_trading_day(now)

            start_time = datetime.strptime(start_time_str, "%H:%M").time()
            end_time = datetime.strptime(end_time_str, "%H:%M").time()

            # 构建检查时间窗口 (盘后时间窗口)
            # 盘后定义为：T日收盘后到T+1交易日开盘前
            checkin_start = datetime.combine(today, start_time)
            # 如果结束时间是第二天，则需要加上一天
            if end_time < start_time:
                checkin_end = datetime.combine(next_trading_day.date() if next_trading_day else today, end_time)
            else:
                checkin_end = datetime.combine(today, end_time)

            # 判断当前是否在打卡时间窗口内
            is_in_checkin_window = checkin_start <= now <= checkin_end

            return {
                "is_trading_day": True,
                "is_in_checkin_window": is_in_checkin_window,
                "checkin_start": checkin_start,
                "checkin_end": checkin_end,
                "next_trading_day": next_trading_day,
                "current_time": now,
            }
        else:
            # 今天不是交易日，获取下一个交易日
            next_trading_day = self.get_next_trading_day(now)
            if next_trading_day:
                # 获取下一个交易日的前一交易日（即最近的交易日）
                previous_trading_day = self.get_previous_trading_day(next_trading_day)

                if previous_trading_day:
                    # 使用最近交易日作为参考来构建时间窗口
                    start_time = datetime.strptime(start_time_str, "%H:%M").time()
                    end_time = datetime.strptime(end_time_str, "%H:%M").time()

                    # 构建检查时间窗口（基于最近的交易日）
                    checkin_start = datetime.combine(previous_trading_day.date(), start_time)
                    checkin_end = datetime.combine(next_trading_day.date(), end_time)

                    # 判断当前是否在打卡时间窗口内
                    is_in_checkin_window = checkin_start <= now <= checkin_end

                    return {
                        "is_trading_day": False,
                        "is_in_checkin_window": is_in_checkin_window,
                        "checkin_start": checkin_start,
                        "checkin_end": checkin_end,
                        "next_trading_day": next_trading_day,
                        "current_time": now,
                    }

            return {
                "is_trading_day": False,
                "is_in_checkin_window": False,
                "checkin_start": None,
                "checkin_end": None,
                "next_trading_day": next_trading_day,
                "current_time": now,
            }

    async def can_user_checkin(
        self, user_id: str, group_id: Optional[str] = None, now: Optional[datetime] = None
    ) -> tuple[bool, str]:
        """检查用户是否可以打卡"""
        if now is None:
            now = datetime.now()

        # 检查时间窗口
        status = self.get_current_trading_status(user_id, group_id, now=now)

        if not status["is_in_checkin_window"]:
            if status["checkin_start"] and status["checkin_end"]:
                return False, (
                    f"不在打卡时间窗口内，请在 {status['checkin_start'].strftime('%H:%M')} - "
                    f"{status['checkin_end'].strftime('%m月%d日 %H:%M')} 之间打卡"
                )
            else:
                return False, "今天不是交易日，且未找到合适的打卡时间窗口"

        # 检查今日是否已打卡
        user_data = self.load_user_checkin_data(user_id, group_id)
        today_str = now.strftime("%Y-%m-%d")

        for checkin in user_data["checkins"]:
            if checkin["date"] == today_str:
                # 获取当前和下一个交易日信息
                current_trading_day = now.date()
                # 如果今天是交易日，则显示今天的日期作为当前交易日
                if self.is_trading_day(now):
                    current_trading_day_str = current_trading_day.strftime("%Y年%m月%d日")
                else:
                    # 如果今天不是交易日，获取最近的交易日
                    previous_trading_day = self.get_previous_trading_day(now)
                    current_trading_day_str = (
                        previous_trading_day.strftime("%Y年%m月%d日") if previous_trading_day else "未知"
                    )

                next_trading_day = self.get_next_trading_day(now)
                next_trading_day_str = next_trading_day.strftime("%Y年%m月%d日") if next_trading_day else "未知"

                return False, f"交易日（{current_trading_day_str}）已复盘\n下一个交易日：{next_trading_day_str}"

        return True, "可以打卡"

    def calculate_simple_strike_count(self, checkins: list[dict]) -> int:
        """
        Calculate strike count based on consecutive trading days.
        This is a simplified version that's called when needed, not for every operation.

        Args:
            checkins: All check-ins for the user

        Returns:
            Strike count (consecutive trading days)
        """
        if not checkins:
            return 0

        # Get all unique trading days that were checked in for
        trading_days_checked_in = list({c.get("trading_day") for c in checkins if c.get("trading_day")})

        if not trading_days_checked_in:
            return 0

        # Convert to date objects and sort
        try:
            trading_day_dates = [datetime.strptime(day, "%Y-%m-%d").date() for day in trading_days_checked_in]
            trading_day_dates.sort()
        except ValueError as e:
            logger.error(f"解析交易日期时出错: {e}")
            return 0

        # Count consecutive trading days from the most recent
        strike_count = 0
        current_date = trading_day_dates[-1]

        # Go backwards from the most recent trading day
        for trading_day in reversed(trading_day_dates):
            # If this is the first iteration or the trading day is consecutive
            if strike_count == 0 or (current_date - trading_day).days == 1:
                strike_count += 1
                current_date = trading_day
            else:
                # Break if not consecutive
                break

        return strike_count

    # 创建复盘命令
    @filter.command("复盘")
    async def fupan_checkin(self, event: AstrMessageEvent, conclusion: str = ""):
        """处理复盘命令"""
        # 获取当前时间戳
        now = datetime.now()

        # 获取用户和群组信息
        user_id = event.get_sender_id()
        group_id = event.get_group_id() if event.get_group_id() else None

        # 获取用户昵称
        nickname = event.get_sender_name()

        # 检查是否可以打卡
        can_checkin, message = await self.can_user_checkin(user_id, group_id, now=now)

        if not can_checkin:
            yield event.plain_result(message)
            return

        # 执行打卡
        user_data = self.load_user_checkin_data(user_id, group_id)
        # 更新昵称信息
        user_data["nickname"] = nickname
        today_str = now.strftime("%Y-%m-%d")
        current_timestamp = now.timestamp()
        current_time_str = now.strftime("%Y-%m-%d %H:%M:%S")

        # Determine which trading day this check-in is for
        trading_day = today_str
        next_trading_day_obj = None

        if not self.is_trading_day(now):
            # If today is not a trading day, this check-in is for the previous trading day
            previous_trading_day = self.get_previous_trading_day(now)
            if previous_trading_day:
                trading_day = previous_trading_day.strftime("%Y-%m-%d")
            # Get the next trading day after the trading day this check-in is for
            next_trading_day_obj = self.get_next_trading_day(previous_trading_day if previous_trading_day else now)
        else:
            # If today is a trading day, get the next trading day
            next_trading_day_obj = self.get_next_trading_day(now)

        next_trading_day_str = next_trading_day_obj.strftime("%Y-%m-%d") if next_trading_day_obj else None

        # 添加打卡记录，包含更多详细信息
        checkin_record = {
            "date": today_str,
            "timestamp": current_timestamp,
            "trading_day": trading_day,
            "next_trading_day": next_trading_day_str,
            "context": "group" if group_id else "private",
        }

        # 添加结论文本（如果有）
        if conclusion:
            checkin_record["conclusion"] = conclusion

        # Update strike count efficiently by comparing with previous check-in
        if len(user_data["checkins"]) > 0:
            # Get the previous check-in (the one before we add the new one)
            previous_checkin = user_data["checkins"][-1]
            previous_next_trading_day = previous_checkin.get("next_trading_day")
            previous_trading_day = previous_checkin.get("trading_day")

            # Special case: if this check-in is for the same trading day as the previous one,
            # it doesn't change the strike count (same trading day)
            if trading_day == previous_trading_day:
                # Same trading day, strike count unchanged
                pass
            # If this check-in's trading day matches the previous check-in's next trading day,
            # it's a consecutive strike
            elif previous_next_trading_day and trading_day == previous_next_trading_day:
                user_data["strike_count"] += 1
            else:
                # If not consecutive, reset to 1 (this check-in starts a new streak)
                user_data["strike_count"] = 1
        else:
            # First check-in, start with strike count of 1
            user_data["strike_count"] = 1

        user_data["checkins"].append(checkin_record)
        user_data["total_count"] = len(user_data["checkins"])

        # 保存数据
        self.save_user_checkin_data(user_id, user_data, group_id)

        # 获取当前和下一个交易日信息
        current_trading_day = now.date()
        # 如果今天是交易日，则显示今天的日期作为当前交易日
        if self.is_trading_day(now):
            current_trading_day_str = current_trading_day.strftime("%Y年%m月%d日")
        else:
            # 如果今天不是交易日，获取最近的交易日
            previous_trading_day = self.get_previous_trading_day(now)
            current_trading_day_str = previous_trading_day.strftime("%Y年%m月%d日") if previous_trading_day else "未知"

        next_trading_day = self.get_next_trading_day(now)
        next_trading_day_str = next_trading_day.strftime("%Y年%m月%d日") if next_trading_day else "未知"

        # 发送成功消息，包含当前和下一个交易日信息
        strike_count = user_data.get("strike_count", 0)
        success_msg = (
            f"✅ 复盘成功！\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"  复盘时间：{current_time_str}\n"
            f"  累计复盘：{user_data['total_count']}次\n"
            f"  连续复盘：{strike_count}连击\n"
        )

        # 添加结论显示（如果有）
        if conclusion:
            success_msg += f"  复盘结论：{conclusion}\n"

        success_msg += (
            f"━━━━━━━━━━━━━━━━\n  交易日（{current_trading_day_str}）已复盘\n  下一个交易日：{next_trading_day_str}"
        )
        yield event.plain_result(success_msg)

    # 统计命令
    @filter.command("复盘统计", alias={"复盘 stats"})
    async def fupan_stats(self, event: AstrMessageEvent):
        """处理复盘统计命令"""
        user_id = event.get_sender_id()
        group_id = event.get_group_id() if event.get_group_id() else None
        user_data = self.load_user_checkin_data(user_id, group_id)
        context = "群组" if group_id else "私聊"

        # Get basic statistics
        total_checkins = user_data["total_count"]
        strike_count = user_data.get("strike_count", 0)

        stats_msg = f"📈 您的{context}复盘统计\n"
        stats_msg += "━━━━━━━━━━━━━━━━\n"
        stats_msg += f"  总复盘次数：{total_checkins}次\n"
        stats_msg += f"  连续复盘次数：{strike_count}连击\n"
        stats_msg += "━━━━━━━━━━━━━━━━\n\n"

        # Add recent check-in history (last 10)
        if user_data["checkins"]:
            stats_msg += "📚 最近复盘记录：\n"
            # Sort checkins by date descending to show most recent first
            sorted_checkins = sorted(user_data["checkins"], key=lambda x: x["date"], reverse=True)
            for i, checkin in enumerate(sorted_checkins[:10]):  # Show last 10
                date_str = checkin["date"]
                # Format the date to be more readable
                formatted_date = date_str.replace("-", "年", 1).replace("-", "月", 1) + "日"
                context_type = "群" if checkin.get("context") == "group" else "私"

                # Add conclusion if available
                if checkin.get("conclusion"):
                    stats_msg += f"  {i + 1}. {formatted_date} ({context_type})\n     复盘：{checkin['conclusion']}\n"
                else:
                    stats_msg += f"  {i + 1}. {formatted_date} ({context_type})\n"
        else:
            stats_msg += "📚 暂无复盘记录\n"

        yield event.plain_result(stats_msg)

    # 排行命令
    @filter.command("复盘排行", alias={"复盘 rank"})
    async def fupan_rank(self, event: AstrMessageEvent):
        """处理复盘排行命令"""
        group_id = event.get_group_id() if event.get_group_id() else None
        all_files = self.get_all_checkin_files()
        rank_data = []

        # 根据当前环境过滤文件（群组或私聊）

        for file_path in all_files:
            # 只处理与当前环境匹配的文件
            if (group_id and f"_group_{group_id}.json" in file_path) or (
                not group_id and file_path.endswith("_dm.json")
            ):
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        # Ensure strike_count field exists for backward compatibility
                        if "strike_count" not in data:
                            data["strike_count"] = 0
                        rank_data.append(
                            {
                                "user_id": data["user_id"],
                                "nickname": data.get("nickname", data["user_id"]) or data["user_id"],
                                "count": data["strike_count"],  # Use strike count for ranking
                            }
                        )
                except (OSError, IOError, json.JSONDecodeError) as e:
                    logger.error(f"读取排行数据文件 {file_path} 时出错: {e}")
                    # Continue with other files

        # 按连续打卡次数排序
        rank_data.sort(key=lambda x: x["count"], reverse=True)

        context = "群组" if group_id else "私聊"
        rank_msg = f"🏆 {context}复盘连续打卡排行\n"
        rank_msg += "━━━━━━━━━━━━━━━━\n"

        if not rank_data:
            rank_msg += "  暂无数据\n"
        else:
            for i, data in enumerate(rank_data[:10], 1):  # 显示前10名
                # 如果nickname与user_id相同或为空，显示"用户{user_id}"格式
                if not data["nickname"] or data["nickname"] == data["user_id"]:
                    display_name = f"用户{data['user_id']}"
                else:
                    display_name = data["nickname"]
                rank_msg += f"  {i}. {display_name}: {data['count']}连击\n"

        rank_msg += "━━━━━━━━━━━━━━━━"

        yield event.plain_result(rank_msg)

    # 撤销复盘命令
    @filter.command("复盘撤销", alias={"撤销复盘"})
    async def fupan_revoke(self, event: AstrMessageEvent):
        """处理撤销复盘命令"""
        # 获取用户信息
        user_id = event.get_sender_id()
        group_id = event.get_group_id() if event.get_group_id() else None

        # 加载用户数据
        user_data = self.load_user_checkin_data(user_id, group_id)

        # 检查是否有复盘记录
        if not user_data["checkins"]:
            yield event.plain_result("您还没有任何复盘记录，无需撤销")
            return

        # 获取最后一个复盘记录
        last_checkin = user_data["checkins"][-1]
        last_checkin_date = last_checkin["date"]
        # Convert timestamp to readable time format
        last_checkin_timestamp = last_checkin["timestamp"]
        last_checkin_time = datetime.fromtimestamp(last_checkin_timestamp).strftime("%Y-%m-%d %H:%M:%S")

        # 移除最后一个打卡记录
        user_data["checkins"].pop()
        user_data["total_count"] = len(user_data["checkins"])

        # Recalculate strike count after revoking
        # For simplicity in this less frequent operation, we'll recalculate using the full sequence
        if len(user_data["checkins"]) > 0:
            user_data["strike_count"] = self.calculate_simple_strike_count(user_data["checkins"])
        else:
            # No check-ins left, reset strike count
            user_data["strike_count"] = 0

        # 保存更新后的数据
        self.save_user_checkin_data(user_id, user_data, group_id)

        # 发送成功消息
        strike_count = user_data.get("strike_count", 0)
        revoke_msg = (
            f"✅ 已成功撤销最后一次复盘\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"  撤销记录：{last_checkin_date} {last_checkin_time}\n"
            f"  当前累计复盘：{user_data['total_count']}次\n"
            f"  连续复盘：{strike_count}连击\n"
            f"━━━━━━━━━━━━━━━━"
        )
        yield event.plain_result(revoke_msg)

    # 数据重置命令
    @filter.command("复盘重置")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def fupan_reset(self, event: AstrMessageEvent, arg: str = ""):
        """处理复盘数据重置命令"""
        group_id = event.get_group_id() if event.get_group_id() else None

        if arg == "私聊":
            # 重置所有私聊数据
            count = self.reset_all_dm_data()
            yield event.plain_result(f"✅ 已重置所有私聊用户的复盘数据，共重置 {count} 位用户的记录")
        elif arg == "当前群组" and group_id:
            # 重置当前群组数据
            count = self.reset_group_data(group_id)
            yield event.plain_result(f"✅ 已重置群组 {group_id} 的复盘数据，共重置 {count} 位用户的记录")
        elif arg.startswith("群组"):
            # 重置指定群组数据
            target_group_id = arg[2:].strip()  # 去掉"群组"前缀
            if target_group_id:
                count = self.reset_group_data(target_group_id)
                yield event.plain_result(f"✅ 已重置群组 {target_group_id} 的复盘数据，共重置 {count} 位用户的记录")
            else:
                yield event.plain_result("❌ 请提供要重置的群组ID，例如：/复盘重置 群组123456")
        else:
            context = "当前群组" if group_id else "私聊"
            yield event.plain_result(
                f"📈 复盘数据重置命令\n"
                f"用法：\n"
                f"  /复盘重置 私聊 - 重置所有私聊用户数据\n"
                f"  /复盘重置 当前{context} - 重置{context}数据\n"
                f"  /复盘重置 群组<群号> - 重置指定群组数据\n"
                f"⚠️ 注意：此操作不可逆，请谨慎使用！"
            )

    # 帮助命令
    @filter.command("复盘帮助", alias={"复盘 help"})
    async def fupan_help(self, event: AstrMessageEvent):
        """处理复盘帮助命令"""
        group_id = event.get_group_id() if event.get_group_id() else None
        context = "群组" if group_id else "私聊"

        help_msg = (
            "📈 复盘插件帮助\n"
            "────────────────\n"
            "📝 基本命令：\n"
            "  /复盘 [复盘结论] - 每日复盘（可附加结论）\n"
            "  /复盘统计 - 个人复盘统计\n"
            "  /复盘排行 - 复盘排行榜\n\n"
            "↩️ 其他命令：\n"
            "  /复盘撤销 | /撤销复盘 - 撤销最后复盘\n"
            f"  /复盘重置 - 重置数据（仅管理员）\n"
            f"  /复盘帮助 - 显示此帮助\n\n"
            f"示例：/复盘 我觉得明天高开低走\n"
            f"当前环境：{context}\n"
            "────────────────\n"
            "💡 数据在群聊和私聊中分开统计"
        )

        yield event.plain_result(help_msg)

    async def terminate(self):
        """插件卸载时调用"""
        logger.info("复盘打卡插件已卸载")
