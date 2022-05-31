library(tidyverse)
library(tidyquant) # Ensure v 4.20
library(modelr)


to <- today()
from <- to - years(5)

kr <- "KR" %>%
  tq_get(get = "stock.prices",
         periodicity = "daily",
         from = from,
         to = to) %>% 
  mutate(index = as.numeric(date)) %>% 
  mutate(dayofweek = wday(date, label = TRUE),
         dayofmonth = mday(date),
         month = month(date))

wmt <- "WMT" %>%
  tq_get(get = "stock.prices",
         periodicity = "daily",
         from = from,
         to = to) %>% 
  mutate(index = as.numeric(date)) %>% 
  mutate(dayofweek = wday(date, label = TRUE),
         dayofmonth = mday(date),
         month = month(date))

cost <- "cost" %>%
  tq_get(get = "stock.prices",
         periodicity = "daily",
         from = from,
         to = to) %>% 
  mutate(index = as.numeric(date)) %>% 
  mutate(dayofweek = wday(date, label = TRUE),
         dayofmonth = mday(date),
         month = month(date))

stocks <- bind_rows(kr, wmt, cost)

lw1 <- loess(close ~ as.numeric(date), data = kr, span = 0.1)
lw2 <- loess(close ~ as.numeric(date), data = kr, span = 0.15)
lw3 <- loess(close ~ as.numeric(date), data = kr, span = 0.05)

kr %>%
  gather_predictions(lw1, lw2, lw3) %>%
  ggplot(aes(date, close, colour=model)) +
  geom_line(colour = "black") +
  geom_line(aes(date, pred))

kr %>%
  add_residuals(lw1) %>%
  ggplot(aes(date, resid)) +
  geom_line()

kr <- kr %>%
  kr %>%
ggplot(aes(month, resid, group=month)) +
geom_boxplot()
add_residuals(lw1) %>%
  mutate(dayofweek = wday(date, label = TRUE),


kr %>%
  ggplot(aes(month, resid, group=month)) +
  geom_boxplot()

kr %>%
  ggplot(aes(dayofweek, resid)) +
  geom_boxplot()

# The data suggest that the lowest price on an arbitary day
# is on a Thursday. But what time on a Thursday?
kr %>%
  group_by(dayofweek) %>%
  summarise(across(
    .cols = c(open, high, low, close),
    .fns = list(mean = mean, md = median, sd = sd)
  ))






