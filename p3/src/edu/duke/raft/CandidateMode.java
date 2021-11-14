package edu.duke.raft;

import java.util.Timer;
import java.util.concurrent.ThreadLocalRandom;

/* NOTES
 * The RaftResponses class is very useful - it can keep track of all server votes
 */

public class CandidateMode extends RaftMode {
  // ID for timer
  private static final int ELECTION_ID = 0;
  // Polling values
  private static final int POLL_TIMEOUT = 10;
  private static final int POLL_ID = 1;

  // Have two timers. One to poll election results, the other to check election timeout
  private Timer electionTimer;
  private Timer pollingTimer;

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
    electionTimer = super.scheduleTimer(getTimeout(), ELECTION_ID);
  }

  public void go() {
    synchronized (mLock) {
      // Get the server's current term
      int term = mConfig.getCurrentTerm();
      // Immediately increment the term and set that the server voted for itself
      mConfig.setCurrentTerm(++term, mID);
      System.out.println("S" + mID + "." + term + ": switched to candidate mode.");

      // Initialize the Responses object
      RaftResponses.init(mConfig.getNumServers(), term);
      // Immediately set vote for self. The "initial" round is -1

      // TODO: Check that this actually correctly sets the vote
      RaftResponses.setVote(mID, 0, term, -1);

      // Servers are 1-indexed
      for(int i = 1; i <= mConfig.getNumServers(); i++) {
        if (i == mID) {
          // Don't send vote request to self
          continue;
        }
        // RPC to request a vote
        remoteRequestVote(i, term, mID, mLog.getLastIndex(), mLog.getLastTerm());
      }
      // Timer to check voting status
      pollingTimer = scheduleTimer(POLL_TIMEOUT, POLL_ID);

      // Election timeout timer
      startTimer();
    }
  }

  // @param candidate’s term
  // @param candidate requesting vote
  // @param index of candidate’s last log entry
  // @param term of candidate’s last log entry
  // @return 0, if server votes for candidate; otherwise, server's
  // current term 
  public int requestVote(int candidateTerm,
                         int candidateID,
                         int lastLogIndex,
                         int lastLogTerm) {
    synchronized (mLock) {
      int term = mConfig.getCurrentTerm();
      int result = term;

      // If get a request to append from a higher-termed candidate, vote for the candidate instead
      if (candidateTerm > term) {
        pollingTimer.cancel();
        electionTimer.cancel();
        // Set that voted for the candidate
        mConfig.setCurrentTerm(candidateTerm, candidateID);
        // Step down to be a follower
        RaftServerImpl.setMode(new FollowerMode());  // TODO: Does this let the function return?
        System.out.println("DOES THIS CODE EXECUTE?");
        // Vote for the requesting candidate
        // TODO: Should the candidate vote? Should check the log first?
        return 0;
      }


      return result;
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
  public int appendEntries(int leaderTerm,
                           int leaderID,
                           int prevLogIndex,
                           int prevLogTerm,
                           Entry[] entries,
                           int leaderCommit) {
    synchronized (mLock) {
      int term = mConfig.getCurrentTerm();
      int result = term;

      // If get a request to append from a higher-termed leader, cancel the election
      if (leaderTerm > term) {
        pollingTimer.cancel();
        electionTimer.cancel();
        mConfig.setCurrentTerm(leaderTerm, 0);
        RaftServerImpl.setMode(new FollowerMode());
        return leaderTerm;
      }

      return result;
    }
  }

  // @param id of the timer that timed out
  public void handleTimeout(int timerID) {
    synchronized (mLock) {
      switch (timerID) {
        case ELECTION_ID:
          break;

        case POLL_ID:
          // Read through all responses.
          int[] votes = RaftResponses.getVotes(mConfig.getCurrentTerm());
          // Keep track of voters for this candidate
          int numChosen = 0;

          for(int vote : votes) {
            if (vote == 0){
              numChosen++;
            }
            if(vote > mConfig.getCurrentTerm()) {
              // Encountered a response with a higher term. Immediately drop to follower status
              electionTimer.cancel();
              // Update own term. Did not vote for any server
              mConfig.setCurrentTerm(vote, 0);
              RaftServerImpl.setMode(new FollowerMode());
            }
            // If the majority of servers voted for this candidate, become leader
            if(numChosen > ((float)mConfig.getNumServers() / 2)) {
              electionTimer.cancel();
              RaftServerImpl.setMode(new LeaderMode());
            }
            // Otherwise, reset the poll timer again
            else {
              scheduleTimer(POLL_TIMEOUT, POLL_ID);
            }
          }
          break;
        default:
          throw new RuntimeException("Bad timer value");
      }
    }
  }
}
