# Commodity Inflation Factor
The overall goal is to develop cross-sectional L/S factors for commodities that are sensitive to inflation. This is done via commodity futures and finding those commodities that are sensitive to inflation premia.

## Background
Cross-sectional factors are widely used for various reasons such as risk management and factor trading. Most of the research within cross-sectional factors are equities (such as value and growth), and with some recent literature within fixed income. Factors within the macro space are relatively sparse. This repo builds a commodity-based factor trying to isolate the inflation risk premia. 

The methodology used in this repo could be applied to other products such as equities, fixed income, and currencies. Although due to the limited number of fixed income products (specifically futures) developing cross-sectional factors may be difficult. While most factor models presume some sort of formalized model or charateristic value/function (i.e. P/E ratios) this factor relies on causal relationship. 

An easy workaround for developing factors *on-the-fly* is to exploit the fact securities that have specific embedded risk premias should have betas to those risk premias. Or simply put, building a cross-sectional beta factor against regressors capture that risk premia. In this case the commodity futures are regressed upon an inflation-based measurement. 

## Choice of Inflation Measurement
While many inflation measurements exist, and CPI measurements could work, there are two that standout. The first is the treasury breakeven curve and the inflation swaps curve. In this case each curve is decomposed into its relevant factors, via Principal Component Anaylsis (PCA), and then regressed against the commodities. 

## Factor Construction
See at the bottom in appendices for specific futures contracts. 
1. First begin with decomposing the two term structure to their principal components respectively.
2. Then regress those fitted values against the returns of the commodites.
3. Quartile the betas to build cross-sectional long-short legs
4. Exponential weight each quartile so that the highest ranked/lowest securities get the most long/short weigthing.

All portfolios are dollar neutral. In theory $\beta$ inflation can be hedged by re-weighting the legs via their necessary $\beta$-hedge i.e. solving for this equation $w_{Q,long} \cdot \beta_{Q,long} - w_{short} \cdot \beta_{Q,short} = 0$ such that
$|w_{Q,long}| + |w_{Q,short}| = 0$. This is really an inflation-neutral portfolio (which is not technically market neutral), the same approach could be applied to the Bloomberg Commodities Index. 

There are two current versions of the factor model
1. Daily Quartiling & Daily Rebalance - this means that the quartiling / ranking is done every day as well as the rabalance back to the expoential weightings.
2. Monthly Quartiling & Daily Rebalance - this means that the securities are picked (via quartiling) monthly but the exponential weightings are rebalanced daily.

## Pre-Processing
First start with the decomposition of the Breakeven Curve and & Inflation Swap Curve via PCA
![image](https://github.com/user-attachments/assets/2aedf8d8-bc45-4a6a-a73b-360c37a2d9b9)

Looking at the cumulative explained variance of the PCs
![image](https://github.com/user-attachments/assets/e59841c8-252d-4963-9e29-b6539f501cfb)

And their corresponding loadings
![image](https://github.com/user-attachments/assets/b2a5386f-2df6-4241-bbb0-0a13beda7d0f)

## Monthly Quartiled Security & Daily Weighting Rebalance
First start with cumulative return of each L/S on a long-only basis
![image](https://github.com/user-attachments/assets/3850f329-37ea-4014-942f-5bb83db658e2)

It's corresponding sharpe values can be calculated
![image](https://github.com/user-attachments/assets/78ea3985-8a65-4b1c-8739-a84c7dced24c)

The cumulative performance of each factor per each PC of each curve is
![image](https://github.com/user-attachments/assets/504c0ea7-5753-4ef3-82ea-54f14a2e66d8)

The sharpes of each spread
![image](https://github.com/user-attachments/assets/1993ad54-1d96-4405-b2e8-4b63527a5185)


## Appendices
Collection of Futures Contracts
| Contract   | Description      | Start Date   | End Date   |
|:-----------|:-----------------|:-------------|:-----------|
| BO1        | Soybean Oil      | 1990-01-03   | 2024-10-29 |
| C1         | Corn             | 1990-01-03   | 2024-10-29 |
| CC1        | Cocoa            | 1990-01-03   | 2024-10-29 |
| CL1        | WTI Crude        | 1990-01-03   | 2024-10-29 |
| CO1        | Brent Crude      | 1990-01-03   | 2024-10-29 |
| CT1        | Cotton           | 1990-01-03   | 2024-10-29 |
| GC1        | Gold             | 1990-01-03   | 2024-10-29 |
| HG1        | Copper           | 1990-01-03   | 2024-10-29 |
| HO1        | Heating Oil      | 1990-01-03   | 2024-10-29 |
| KC1        | Coffee           | 1990-01-03   | 2024-10-29 |
| LA1        | Aluminum         | 1997-07-25   | 2024-10-29 |
| LC1        | Lean Cattle      | 1990-01-03   | 2024-10-29 |
| LH1        | Lean Hogs        | 1990-01-03   | 2024-10-29 |
| LN1        | Nickel           | 1997-07-25   | 2024-10-29 |
| LX1        | Zinc             | 1997-07-25   | 2024-10-28 |
| NG1        | Natural Gasoline | 1990-04-05   | 2024-10-29 |
| QS1        | Gas Oil          | 1990-01-03   | 2024-10-29 |
| QW1        | Sugar            | 1990-01-03   | 2024-10-29 |
| S1         | Soybean          | 1990-01-03   | 2024-10-29 |
| SI1        | Silver           | 1990-01-03   | 2024-10-29 |
| SM1        | Soybean Meal     | 1990-01-03   | 2024-10-29 |
| W1         | Wheat            | 1990-01-03   | 2024-10-29 |
| XB1        | Gasoline         | 2005-10-05   | 2024-10-29 |
