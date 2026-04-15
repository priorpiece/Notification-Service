package kafka

/** 시간 기반 키 유틸 (문자열 정렬 == 시간 오름차순) */
object TimeKey {
  // 19자리 zero-padding + '|' + scheduleId
  def build(reminderTime: Long, scheduleId: String): String =
    f"$reminderTime%019d|$scheduleId"

  // now 이하 상한 키 (상한자는 '~')
  def upperBound(now: Long): String =
    f"$now%019d|~"

  // 최하한 키
  val lowerBound: String = "0000000000000000000|"
}