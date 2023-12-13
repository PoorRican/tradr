# Data Handlers

- [X] Ensure that data is handled correctly
- [X] Create a script to constantly download and update local datasets
- [X] Remove all db references and revert back to using local files
- [X] Candle data should be stored individually by frequency

# Market

- [ ] Implement websockets instead of polling. This should lead to more accurate data for shorter timeframes (ie: 1m, 5m, etc.)

# Indicators

- [ ] `MACDRow._row_decision` and `MACD._row_strength` are overly simple and should be replaced with a better way to evaluate the decision and calculate the strength
- [ ] Calculating strength from indicators is a bad idea. Indicators should only be used to make decisions. The entire 'strength' concept should be removed.
- [ ] The arguments passed to `Indicator._row_decision` should exclusively be a `DataFrame`, not a `Series`.
- [ ] There should be a way to plot indicator decisions on a chart.
- [X] Rename `FrequncySignal` to `IndicatorGroup`
- [X] Rename `IndicatorGroup._compute()` to `IndicatorGroup._compute_signals()`
- [X] Rename `IndicatorGroup._process()` to `IndicatorGroup._generate_indicator_graph()`
- [ ] Improve `BBandsRow._row_decision()` to be only give `SELL` or `BUY` signal more closely to bounds
- [ ] Allow `IndicatorGroup` to be passed a function to evaluate buy signals. That way strategies can have more control over how to evaluate signals from indicators. For example, BollingerBands should restrict the overall output.

# Strategies

- [ ] `FinancialsMixin` should be used as an attribute instead of a mixin.

# Analysis

- [ ] `TrendDetector` should use ranking of frequencies instead of a simple "mean". For example, if 1day is `SELL`, and `1h` is `BUY`, then the final decision should lean more towards `BUY`.
- [ ] `TrendDetector` should use a better indicator such as a combination of Bollinger Bands and EMA to determine the trend
- [ ] Add legends to plotting
- [ ] Remove assets and amt from graph. Or at least plot this graph at the end.
