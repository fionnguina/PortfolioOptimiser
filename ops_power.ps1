<#
.SYNOPSIS
    Keep the machine awake for the duration of an unattended run.

.DESCRIPTION
    Dot-source this, call Suspend-IdleSleep at the top of a wrapper and
    Resume-IdleSleep at the bottom.

    WHY (2026-08-07): the evening evidence run launched at 18:00, the machine
    idle-slept a few minutes later, and the wrapper came back at 09:59 the next
    morning to kill an engine that had barely run. That evening's
    scale-sensitivity sample was lost. The scheduled task already had
    WakeToRun=True, which wakes the box to START a task and does nothing at all
    about sleeping DURING one.

    HOW: SetThreadExecutionState with ES_SYSTEM_REQUIRED tells Windows the
    thread is doing work, so the idle timer never fires. ES_CONTINUOUS makes it
    stick until explicitly cleared rather than resetting after one idle check.
    ES_DISPLAY_REQUIRED is deliberately NOT set — the screen should still blank;
    only the system must stay up.

    LIMITS, so nobody trusts this further than it goes:
      - It blocks IDLE sleep only. A deliberate sleep (lid close, Start > Sleep,
        a power-button press) still suspends the machine.
      - It is per-THREAD. It holds only while the calling PowerShell thread
        lives, which is exactly the wrapper's lifetime — and if the wrapper is
        killed, the requirement dies with the process, so it cannot leak.
      - Hibernate on battery timeout is a separate policy and is not affected.
#>

function Suspend-IdleSleep {
    <#  Returns $true if the request was accepted. Non-fatal on failure: a run
        that cannot block sleep is still a run worth attempting. #>
    try {
        if (-not ("Win32.OpsPower" -as [type])) {
            Add-Type -Namespace Win32 -Name OpsPower -MemberDefinition @'
[DllImport("kernel32.dll", SetLastError = true)]
public static extern uint SetThreadExecutionState(uint esFlags);
'@
        }
        # ES_CONTINUOUS (0x80000000) | ES_SYSTEM_REQUIRED (0x00000001)
        $prev = [Win32.OpsPower]::SetThreadExecutionState([uint32]'0x80000001')
        # 0 means the call failed; any other value is the previous state.
        return ($prev -ne 0)
    } catch {
        return $false
    }
}

function Resume-IdleSleep {
    <#  Clear the requirement — ES_CONTINUOUS alone restores normal idle
        behaviour. Safe to call even if Suspend-IdleSleep failed. #>
    try {
        if ("Win32.OpsPower" -as [type]) {
            [void][Win32.OpsPower]::SetThreadExecutionState([uint32]'0x80000000')
        }
    } catch { }
}
