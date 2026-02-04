                                                                   
  ┌─────────────────────────────────────────────────────────────┐  
  │                      SCHEDULER HOOKS                        │  
  ├─────────────────────────────────────────────────────────────┤  
  │ casgstatus()     → changegstatus()    │ track running      │   
  │ park_m()         → inc/decActive()    │ transient protect  │   
  │ newproc1()       → inherit bubble     │ child gets bubble  │   
  │ entersyscall()   → force casgstatus   │ syscall tracking   │   
  ├─────────────────────────────────────────────────────────────┤  
  │                        TIME HOOKS                           │  
  ├─────────────────────────────────────────────────────────────┤  
  │ time.Now()       → return bubble.now  │ fake time          │   
  │ time.Sleep()     → bubble.timers      │ fake sleep         │   
  │ startTimer()     → t.isFake = true    │ fake timer         │   
  ├─────────────────────────────────────────────────────────────┤  
  │                      CHANNEL HOOKS                          │  
  ├─────────────────────────────────────────────────────────────┤  
  │ makechan()       → ch.bubble = ...    │ associate channel  │   
  │ chansend/recv()  → check bubble match │ prevent cross-talk │   
  │ selectgo()       → check all channels │ prevent cross-talk │   
  └─────────────────────────────────────────────────────────────┘  
                                                                   
  Deadlock Detection                                               
                                                                   
  maybeWakeLocked():                                               
      if running > 0 || active > 0:                                
          return  // still active                                  
                                                                   
      if waiter != nil:                                            
          wake waiter  // synctest.Wait() returns                  
      else if next_timer > 0:                                      
          wake root    // advance time                             
      else if total > 1:                                           
          DEADLOCK!    // Gs blocked, no timers, no progress       
                                                             