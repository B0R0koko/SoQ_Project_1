<h2>SOQ Project 1. </h2>
<h4>Mironov Mikhail</h4>

<h4>Reference sources</h4>
<ul>
    <li><a href="https://www.econstor.eu/bitstream/10419/116783/1/833997289.pdf">Statistical arbitrage pairs trading strategies: Review and outlook</a></li>
</ul>

<h4>Notes</h4>
<p>The project is largely unfinished, I tried my best to make it to the deadline. I have done plenty of work and found a new interesting approach to trading.</p>

<h4>Scrapy notes and Cointegration</h4>

<ul>
    <li>To collect kline data from binance, run "scrapy runspider parsers/kline_parser.py"</li>
    <li>To collect CMC data, run "scrapy runspider parsers/cmc_spider.py"</li>
    <li>In order to change output directories of data, have a look at settings.py file</li>
    <li>To find cointegrated pairs run python cointegration.py, the default number of processes is set tp 15</li>
</ul>