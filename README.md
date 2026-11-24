# pump-fun-paper-sprint-bot

[![make.fun · u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b.svg)](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b?utm_source=github&utm_medium=readme&utm_campaign=u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)
[![rank](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/tuyulsembilanenam672-cell/pump-fun-paper-sprint-bot/rank.svg)](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/leaderboard?utm_source=github&utm_medium=readme&utm_campaign=u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)
[![reward](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/tuyulsembilanenam672-cell/pump-fun-paper-sprint-bot/reward.svg)](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b?utm_source=github&utm_medium=readme&utm_campaign=u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)

A deterministic momentum bot for the Pump.fun Paper Sprint: it watches every
fresh coin in the replay's batch stream and buys a small fixed slice
(0.02 SOL) only of the coins still rising near their own peak — up 2% from
creation, within 5% of the peak since creation, at least 8 recorded buys,
and a real reserve of 3 SOL or more — then sells on a +15% take-profit, a
−10% stop-loss, a trailing stop (6% under the peak once up 4%), or a
400-slot hold timeout, whichever comes first. One round trip per coin, at
most 5 positions open, fully deterministic — no clock, no randomness, no
network — so two runs over the same window produce byte-identical action
logs.
