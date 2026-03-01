#!/bin/bash -x

#############################################
# Put this at home directory
#############################################
/home/ubuntu/hl-visor run-non-validator  \
       	--write-trades \
	--write-fills  \
	--write-raw-book-diffs \
	--batch-by-block \
	--disable-output-file-buffering

# Flags:
#   --write-order-statuses
#      Order statuses are a per-block feed of every order lifecycle event across the entire exchange — new orders placed, cancellations, rejections, fills, etc. for all users on all coins.
#  
#      As you can see from the data:
#      - ~570 events per block across all 229 coins
#      - Status types include: open (new order placed), canceled, filled, badAloPxRejected (ALO price crossed), perpMarginRejected, etc.
#      - BTC and ETH dominate, but the vast majority of the data is for coins you don't care about
#  
#      Use cases for order statuses:
#      - Order fill confirmation — tracking when your own orders get filled
#      - Market microstructure analysis — observing order flow, rejection rates, etc.
#      - Strategy backtesting — replaying the full order lifecycle
#  
#      For your use case (book and trade latency for BTC/ETH),
#       you don't need this flag. Your C++ client reads raw_book_diffs (for L2 book updates) and extracts trades from fills. Order statuses are an
#      entirely separate feed.
#  
#      Removing --write-order-statuses would eliminate 6.7 GB/hour of disk writes — by far the largest output. That said, it likely won't fix the 10-second periodic stall (which is internal CPU
#      computation), but it will significantly reduce disk I/O pressure and could help overall node performance.
#
#
#    --serve-info \
#      Adding this would cause a 2~2.5s stall every 10seconds because the non-validator node would try to dump snapshot every 10 seconds
#      and would hang the node for 2~2.5 seconds.
#
#    --stream-with-block-info \
