package edu.duke.raft;

import java.util.Timer;

public class LeaderMode extends RaftMode {
  // How often to send hearbeat messages?
  private static final int HB_INTERVAL = 50;
  private static final int HB_ID = 3;

  private Timer hbTimer;
  private int[] followerLogIndex;
  private boolean mIsDead = false;

  private void sendHeartBeat() {
    for(int i = 1; i <= mConfig.getNumServers(); i++) {
      if(i == mID) {
        continue;
      }
      remoteAppendEntries(i, mConfig.getCurrentTerm(), mID, mLog.getLastIndex(),
          mLog.getLastTerm(), new Entry[0], mCommitIndex);
    }
  }
  private void send(){
     for(int i=1; i<=mConfig.getNumServers(); i++) {
        if(i != mID) {
          int diff = mLog.getLastIndex() - followerLogIndex[i];
          Entry[] entries = new Entry[diff];
          for(int j=0; j<diff; j++) {
              entries[j] = mLog.getEntry(followerLogIndex[i] + j + 1);
          }
          if(mLog.getEntry(followerLogIndex[i]) != null) {
              remoteAppendEntries(i, mConfig.getCurrentTerm(), mID, followerLogIndex[i], mLog.getEntry(followerLogIndex[i]).term, entries, mCommitIndex);
          } else { // if leader log is empty, mLog.getEntry(followerLogIndex[i]) is null, whose term does not exist, so send current term as term.
              remoteAppendEntries(i, mConfig.getCurrentTerm(), mID, followerLogIndex[i], mConfig.getCurrentTerm(), entries, mCommitIndex);
          }
        }
    }

  }
  public void go() {
    synchronized (mLock) {
      System.out.println("S" + mID + "." + mConfig.getCurrentTerm() + ": switched to leader mode.");

      // Immediately send out the heartbeat to stop other elections (and others incrementing their term)
      sendHeartBeat();

      followerLogIndex = new int[mConfig.getNumServers()+1];
      for(int n=0; n <= mConfig.getNumServers(); n++) {
          followerLogIndex[n] = mLog.getLastIndex();
      }

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
      if (mIsDead) return term;
      int vote = term;

      if (candidateTerm > term) {
        hbTimer.cancel();
        // Step down to become a follower
        RaftServerImpl.setMode(new FollowerMode());

        boolean ownLogIsMoreComplete = (mLog.getLastTerm() > lastLogTerm) || ((mLog.getLastTerm() == lastLogTerm) && (mLog.getLastIndex() > lastLogIndex));

        if(!ownLogIsMoreComplete) {
          // Set that voted for the candidate
          mConfig.setCurrentTerm(candidateTerm, candidateID);
          return 0;
        }
        else {
          // Did not vote for the candidate
          mConfig.setCurrentTerm(candidateTerm, 0);
          return term;
        }
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
      if (mIsDead) return term;
      if (leaderTerm > term) {
        hbTimer.cancel();

        RaftServerImpl.setMode(new FollowerMode());

        // Update own term to stay up to date.
        mConfig.setCurrentTerm(leaderTerm, 0);

      }

      return term;
    }
  }

  // @param id of the timer that timed out
  public void handleTimeout(int timerID) {
    synchronized (mLock) {
      if (mIsDead) return;
        
      switch (timerID) {
        case HB_ID:
          hbTimer.cancel();
          //update followerLogIndex
          int[] appendResponses = RaftResponses.getAppendResponses(mConfig.getCurrentTerm());
          if(appendResponses != null) { // if receives response, modify followerLogIndex
          for(int i=1; i<appendResponses.length; i++) {
            if(i != mID) {
                if(appendResponses[i] != 0 && appendResponses[i] <= mConfig.getCurrentTerm()) {
                    if(followerLogIndex[i] != -1) {
                        followerLogIndex[i] -= 1;
                    }
                } else if (appendResponses[i] != 0 && appendResponses[i] > mConfig.getCurrentTerm()) {
                    RaftResponses.clearAppendResponses(mConfig.getCurrentTerm());
                    mConfig.setCurrentTerm(appendResponses[i], 0);
                    mIsDead = true;
                    RaftServerImpl.setMode(new FollowerMode());
                    return;
                } else if(appendResponses[i] == 0) { // response is 0, update lastIndex
                    followerLogIndex[i] = mLog.getLastIndex();
                }
              }
            }
          }
          // Send heartbeats to all servers
          send();
          hbTimer = scheduleTimer(HB_INTERVAL, HB_ID);
          break;
        default:
          throw new RuntimeException("Unexpected timer id: " + timerID);

      }
    }
  }
}
