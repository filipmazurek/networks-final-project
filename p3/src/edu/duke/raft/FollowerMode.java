package edu.duke.raft;

// Timer required for keeping track of timeouts. From RaftMode
import java.util.Timer;
// Random for setting random timeouts
import java.util.concurrent.ThreadLocalRandom;

/* NOTES
 * mConfig is the RaftConfig object available here. It contains not only all general configuration
 *     but also all specific server details - like the the current term.
 * Timers must be cancelled! The provided convenience function spawns a new timer, which is a new
 *     thread. As a result, timers must be cancelled to shut down the thread
 */

public class FollowerMode extends RaftMode {
  // Variable for the timer - signals that no heartbeat has been received.
  private static final int HB_TIMEOUT_ID = 0;
  // Timer for the
  private Timer timer;

  private int getTimeout() {
    // TimeoutOverride will be -1 if it has not been set
    int timeoutTime = mConfig.getTimeoutOverride();
    // Adding 1 to election timeout max because it is exclusive
    int randomTime = ThreadLocalRandom.current().nextInt(RaftMode.ELECTION_TIMEOUT_MIN, RaftMode.ELECTION_TIMEOUT_MAX + 1);

    // Return the appropriate timeout value
    if (timeoutTime > 0) {
      return timeoutTime;
    }
    else {
      return randomTime;
    }
  }

  private void startTimer() {
     timer = scheduleTimer(getTimeout(), HB_TIMEOUT_ID);
  }

  private void grantVote(int candidateTerm, int candidateID) {
    // Restart the timer to not time out
    timer.cancel();
    startTimer();
    // Update own term and set voted for the candidate
    mConfig.setCurrentTerm(candidateTerm, candidateID);
  }

  /* The FollowerMode's go method is what kicks off this Server object switching into this mode
   */
  public void go () {
    synchronized (mLock) {
      System.out.println ("S" + mID + "." + mConfig.getCurrentTerm() + ": switched to follower mode.");
      startTimer();  // Starts the heartbeat timeout timer
    }
  }

  // @param candidate’s term
  // @param candidate requesting vote
  // @param index of candidate’s last log entry
  // @param term of candidate’s last log entry
  // @return 0, if server votes for candidate; otherwise, server's
  // current term
  public int requestVote (int candidateTerm,
			  int candidateID,
			  int lastLogIndex,
			  int lastLogTerm) {
    synchronized (mLock) {
      int term = mConfig.getCurrentTerm();
      // Log completeness defined in Raft lecture slide 20
      boolean ownLogIsMoreComplete = (mLog.getLastTerm() > lastLogTerm) || ((mLog.getLastTerm() == lastLogTerm) && (mLog.getLastIndex() > lastLogIndex));

      // If the candidates term is the same as the current term, then only grant vote if haven't voted yet and
      //   the candidate's log is at least as complete.
      //   Edge case: if already voted for this candidate in this election, grand vote

      // Candidate's term must be at least as much as own term. Start with greater than case
      if(candidateTerm > term) {
        // And if candidate's log is better, grant vote
        if (!ownLogIsMoreComplete) {
          grantVote(candidateTerm, candidateID);
          return 0;
        }
        // Candidate's log is less complete. Deny vote.
        else {
          return term;
        }
      }

      // If own term is the same as the candidate's
      if(term == candidateTerm) {
        // Grant only if haven't voted yet this term or already voted for the candidate and candidate's log is better
        if (((mConfig.getVotedFor() == 0) || (mConfig.getVotedFor() == candidateID)) && !ownLogIsMoreComplete ) {
          grantVote(candidateTerm, candidateID);
          return 0;
        }
        // Deny vote
        else {
          return term;
        }
      }

      // Own term is higher than the candidate's, deny vote
      return term;

    }
  }

  // @param leader’s term
  // @param current leader
  // @param index of log entry before entries to append
  // @param term of log entry before entries to append
  // @param entries to append (in order of 0 to append.length-1)
  // @param index of highest committed entry
  // @return 0, if server appended entries; otherwise, server's
  // current term
  public int appendEntries (int leaderTerm,
			    int leaderID,
			    int prevLogIndex,
			    int prevLogTerm,
			    Entry[] entries,
			    int leaderCommit) {
    synchronized (mLock) {
      // FIXME: currently will accept all entries and restart own timer. Implement appending
      timer.cancel();
      startTimer();

      int term = mConfig.getCurrentTerm ();
      int result = term;

      // Update own term to match the leader. Didn't vote for anyone
      if (leaderTerm > term) {
        mConfig.setCurrentTerm(leaderTerm, 0);
        return 0;
      }

      return 0;
    }
  }

  /* This function is run as soon as the timer has timed out. The timerID will parametrize the
   * function, allowing us to set different timers for different timeout functions.
   */
  // @param id of the timer that timed out
  public void handleTimeout (int timerID) {
    synchronized (mLock) {
      // Run the approproate code
      switch (timerID){
        // For timeouts, make this server go into Candidate mode
        case HB_TIMEOUT_ID:
          timer.cancel();
          RaftServerImpl.setMode(new CandidateMode());
          break;
        default:
          throw new RuntimeException("Unexpected timerID.");
      }
    }
  }
}

