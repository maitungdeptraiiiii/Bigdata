
# Game Analytics – Key Findings

## 1. Engagement Distribution
EngagementLevel
Medium    19374
High      10336
Low       10324

## 2. PlayTimeHours by EngagementLevel
                   count       mean       std       min       25%        50%        75%        max
EngagementLevel                                                                                   
High             10336.0  12.069238  6.914363  0.000630  6.192524  11.981024  17.958875  23.996857
Low              10324.0  12.104915  6.886693  0.000158  6.161530  12.074409  18.022752  23.997838
Medium           19374.0  11.957503  6.929335  0.000115  5.955822  11.992280  17.934651  23.999592

## 3. Correlation Matrix (numeric features)
                           PlayerID       Age  PlayTimeHours  InGamePurchases  SessionsPerWeek  AvgSessionDurationMinutes  PlayerLevel  AchievementsUnlocked
PlayerID                   1.000000 -0.003044       0.000923         0.002321        -0.005944                  -0.001801    -0.001769              0.003190
Age                       -0.003044  1.000000       0.002462        -0.000186         0.008777                  -0.002269     0.001353             -0.001100
PlayTimeHours              0.000923  0.002462       1.000000        -0.006067        -0.003655                  -0.001925    -0.005152              0.003913
InGamePurchases            0.002321 -0.000186      -0.006067         1.000000         0.005132                  -0.003059     0.006524              0.000098
SessionsPerWeek           -0.005944  0.008777      -0.003655         0.005132         1.000000                  -0.000620     0.003257              0.003187
AvgSessionDurationMinutes -0.001801 -0.002269      -0.001925        -0.003059        -0.000620                   1.000000     0.001368             -0.002227
PlayerLevel               -0.001769  0.001353      -0.005152         0.006524         0.003257                   0.001368     1.000000              0.006343
AchievementsUnlocked       0.003190 -0.001100       0.003913         0.000098         0.003187                  -0.002227     0.006343              1.000000

## 4. Cluster Behavior Summary
         PlayTimeHours  SessionsPerWeek  AvgSessionDurationMinutes  InGamePurchases  AchievementsUnlocked  PlayerLevel
Cluster                                                                                                               
0            19.268104         9.292543                  96.069263              0.0             24.741342    47.511682
1             8.029670         9.533162                  93.941078              0.0             24.886103    76.961015
2            11.940694         9.530780                  94.493222              1.0             24.529287    50.027609
3             7.297077         9.580737                  94.344398              0.0             23.887141    23.736493

## 5. Engagement Distribution per Cluster
EngagementLevel      High       Low    Medium
Cluster                                      
0                0.255261  0.263960  0.480779
1                0.264311  0.228977  0.506711
2                0.265390  0.253327  0.481283
3                0.249515  0.284036  0.466449

