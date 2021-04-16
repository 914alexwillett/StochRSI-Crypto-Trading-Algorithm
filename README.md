# StochRSI Crypto Trading Algorithm
This is an automated trading system that buys and sells 
Bitcoin and Ethereum on the Binance.US cryptocurrency exchange.
Using 15min BTC and ETH candlestick data streamed from a Binance websocket,
the system records the close prices into a Sqlite3 database.
Using price data, the system generates techincal indicators,
primarily the Stochastic Relative Strength Index, but also the 200 period
Moving Average and width of each coins' Bollinger Bands. 
The algorithm trade logic uses those technical indicators to
decide when to buy and sell Bitcoin and Ethereum at strategically profitable times.

The database will keep track of all trades made in the transaction history table,
as well as a buy log and sell log that is used in the algorithm's trade logic.
Trades will also be recorded on a seperate order_message_log.txt file
as a backup to verify database accuracy.

There is a batch file that can be periodically ran using a task scheduler
to check if the algorithm is running and if not rerun the program.
Run checks will be logged into the run_check_log.txt file,
and if errors occur, such as being unable to automatically rerun the algorithm,
issues will be written to an error_log.txt file.

If disconnected, upon reconnecting the system will find that last price recorded
and fill the missing values using historical data provided by Binance's client API.
In addition, it will go through the newly added rows and will record
if any missed trade opportunities occured while the program was not running.

The examples folder shows what some of the populated txt files will look like
once the algorithm is ran.

## Usage
There are a few steps to setup the algorithm before running it.
First, sign up for an account with Binance and generate a new
API key, to interact with the Binance client. Replace the API key and secret key
that is given with the placeholders in the BinanceKeys.py file.
A minimum cash deposit of at least $50.00 USD is recommended or else
errors may occur in the trade logic. 

Next, run the create_database.py script to setup the Sqlite3 trade_database.

After that, delete the placeholder contents in each of the txt log files.

Finally, the algorithm is good to go, so run the trade_bot.py script.
Optionally, set up the scheduled run check using your Operating System's
task scheduler.
