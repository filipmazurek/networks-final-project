package edu.duke.raft;

import java.util.Timer;

public class LeaderMode extends RaftMode {
  // How often to send hearbeat messages?
  private static final int HB_INTERVAL = 50;
  private static final int HB_ID = 3;

  private Timer hbTimer;
  private int[] logIndexFollower;
  private boolean isDead = false;


  private void sendHeartBeat() {
    for(int i = 1; i <= mConfig.getNumServers(); i++) {
      if(i == mID) {
        continue;
      }
      remoteAppendEntries(i, mConfig.getCurrentTerm(), mID, mLog.getLastIndex(),
          mLog.getLastTerm(), new Entry[0], mCommitIndex);
    }
  }

  private void sendEntries(){
     for(int i=1; i<=mConfig.getNumServers(); i++) {
        if(i != mID) {
          int diff = mLog.getLastIndex() - logIndexFollower[i];
          Entry[] entries = new Entry[diff];
          for(int j=0; j<diff; j++) {
              entries[j] = mLog.getEntry(logIndexFollower[i] + j + 1);
          }
          // send current term as term if leader log is empty, mLog.getEntry(logIndexFollower[i]) is null
            remoteAppendEntries(i, mConfig.getCurrentTerm(), mID, logIndexFollower[i], 
              mLog.getEntry(logIndexFollower[i])!=null?mLog.getEntry(logIndexFollower[i]).term:mConfig.getCurrentTerm(), 
              entries, mCommitIndex);
        }
    }
  }

  public void receive(String item) {
    synchronized (mLock) {
      int term = mConfig.getCurrentTerm();
      int idx = mLog.getLastIndex() + 1;
      System.out.println("S"+mID + "." + mConfig.getCurrentTerm() + ": Received item " + item + ", label it as " + idx);
      Entry[] ets = {new Entry(idx, term)};
      mLog.append(ets);
      
      logIndexFollower = new int[mConfig.getNumServers() + 1];

      for (int n = 0; n <= mConfig.getNumServers(); n++) {
        logIndexFollower[n] = mLog.getLastIndex();
      }

      sendEntries();
    }
  }

  public void go() {
    synchronized (mLock) {
      System.out.println("S" + mID + "." + mConfig.getCurrentTerm() + ": switched to leader mode.");

      // Immediately send out the heartbeat to stop other elections (and others incrementing their term)
      sendHeartBeat();

      logIndexFollower = new int[mConfig.getNumServers()+1];
      for(int n=0; n <= mConfig.getNumServers(); n++) {
        logIndexFollower[n] = mLog.getLastIndex();
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
      if (isDead) return term;

      int vote = term;

      if (candidateTerm > term) {
        hbTimer.cancel();
        // Step down to become a follower
        RaftServerImpl.setMode(new FollowerMode());

        boolean ownLogIsMoreComplete = (mLog.getLastTerm() > lastLogTerm) || ((mLog.getLastTerm() == lastLogTerm) && (mLog.getLastIndex() > lastLogIndex));
        mConfig.setCurrentTerm(candidateTerm, !ownLogIsMoreComplete?candidateID:0);
        return !ownLogIsMoreComplete?0:term;

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
      if (isDead) return term;
      int result = term;

      if (leaderTerm > term) {
        hbTimer.cancel();

        RaftServerImpl.setMode(new FollowerMode());


        mConfig.setCurrentTerm(leaderTerm, 0);
        if (entries.length == 0) {
          isDead = true;
          RaftServerImpl.setMode(new FollowerMode());
          return (prevLogIndex == mLog.getLastIndex() && prevLogTerm == mLog.getLastTerm())?0:leaderTerm;
        }
        if(mLog.insert(entries, prevLogIndex, prevLogTerm) != -1) {
          result = 0;
        } 
        isDead = true;
        RaftServerImpl.setMode(new FollowerMode());
      }

      return result;

    }
  }

  // @param id of the timer that timed out
  public void handleTimeout(int timerID) {
    synchronized (mLock) {
      if (isDead) return;
        
      switch (timerID) {
        case HB_ID:
          hbTimer.cancel();
          //update followerLogIndex
          int[] appendResponses = RaftResponses.getAppendResponses(mConfig.getCurrentTerm());
          if(appendResponses != null) { // if receives response, modify followerLogIndex
          for(int id=1; id<appendResponses.length; id++) {
            if(id != mID) {
                if(appendResponses[id] != 0 && appendResponses[id] <= mConfig.getCurrentTerm()) {
                    if(logIndexFollower[id] != -1) {
                      logIndexFollower[id] -= 1;
                    }
                } else if (appendResponses[id] != 0 && appendResponses[id] > mConfig.getCurrentTerm()) {
                    RaftResponses.clearAppendResponses(mConfig.getCurrentTerm());
                    mConfig.setCurrentTerm(appendResponses[id], 0);
                    isDead = true;
                    RaftServerImpl.setMode(new FollowerMode());
                    return;
                } else if(appendResponses[id] == 0) { // response is 0, update lastIndex
                  logIndexFollower[id] = mLog.getLastIndex();
                }
              }
            }
          }
          // Send heartbeats to all servers
          sendEntries();

          hbTimer = scheduleTimer(HB_INTERVAL, HB_ID);
          break;
        default:
          throw new RuntimeException("Unexpected timer id: " + timerID);

      }
    }
  }
}
