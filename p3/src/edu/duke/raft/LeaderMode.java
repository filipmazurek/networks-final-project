package edu.duke.raft;

import java.util.Timer;

public class LeaderMode extends RaftMode {
  // How often to send hearbeat messages?
  private static final int HB_INTERVAL = 50;
  private static final int HB_ID = 4;

  private Timer hbTimer;

  public void go() {
    synchronized (mLock) {
      System.out.println("S" + mID + "." + mConfig.getCurrentTerm() + ": switched to leader mode.");

      // Initialize the heartbeat timer
      hbTimer = scheduleTimer(HB_INTERVAL, HB_ID);
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
      int vote = term;

      if (candidateTerm > term) {
        hbTimer.cancel();
        // Update own term to stay up to date.
        mConfig.setCurrentTerm(candidateTerm, 0);
        RaftServerImpl.setMode(new FollowerMode());
        // TODO: same checking as in candidate mode
        System.out.println("DOES THIS EXECUTE? LEADER -> FOLLOWER");
      }

      return vote;
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

      if (leaderTerm > term) {
        hbTimer.cancel();
        // Update own term to stay up to date.
        mConfig.setCurrentTerm(leaderTerm, 0);
        RaftServerImpl.setMode(new FollowerMode());
        // TODO: same checking as in candidate mode
        System.out.println("DOES THIS EXECUTE? LEADER -> FOLLOWER");
      }

      int result = term;
      return result;
    }
  }

  // @param id of the timer that timed out
  public void handleTimeout(int timerID) {
    synchronized (mLock) {
      switch (timerID) {
        case HB_ID:
          // Send heartbeats to all servers
          for(int i = 1; i <= mConfig.getNumServers(); i++) {
            if(i == mID) {
              continue;
            }
            remoteAppendEntries(i, mConfig.getCurrentTerm(), mID, mLog.getLastIndex(),
                mLog.getLastTerm(), new Entry[0], mCommitIndex);
          }
      }
    }
  }
}
