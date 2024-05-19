## Setup ##
using flights, DataFrames, Chain, AnalysisUtils, DataSkimmer, UnicodePlots, Deneb, CairoMakie
using MethodChains, PartialFuns
MethodChains.init_repl()
PartialFuns.init_repl()

const inputp = "../flightdata"
readdir(inputp)[occursin.(r"\.csv$", readdir(inputp))]
const aaarrivals = "american_arrivals.csv"
const aadepartures = "american_departures.csv"
const deltaarrivals = "delta_arrivals.csv"
const deltadepartures = "delta_departures.csv"
const jbarrivals = "jetblue_arrivals.csv"
const jbdepartures = "jetblue_departures.csv"
const unarrivals = "united_arrivals.csv"
const undepartures = "united_departures.csv"
const plotp = "./plots"

## Look at files ##
peekfile(inputp, aaarrivals, n = 12)
peekfile(inputp, aaarrivals, n = 12, rev = true)

## Read American Airlines Arrivals ##
readarrival(inputp, aaarrivals, version = 3) |> describe
# this shows multiple errors, despite attempts to describe time types
# look at error message and try to find error rows, accounting for fuzziness of line references given multithreading
# show(readarrival(inputp, aaarrivals)[11100:11300, [7, 11]], allrows = true)
# all errors show up in cols 7 and 11
# Show rows of columns 7 and 11 where there is a 24:00 for time
@chain readarrival(inputp, aaarrivals) begin
    subset([7, 11] => ByRow((x, y) -> x == "24:00" || y == "24:00"))
end
# correct readarrival function
readarrival(inputp, aaarrivals, version = 4) |> describe
readarrival(inputp, aaarrivals, version = 4)

# @chain readarrival(inputp, aaarrivals, version = 4) begin
#     @aside show(stdout, "text/plain", skim(_))
#     pageddf(1, 20, 5)
# end

# Description of the American arrivals dataset
readarrival(inputp, aaarrivals, version = 4).{
    show(stdout, "text/plain", skim(it))    pageddf(it, 1, 20, 5)
    them
}

# Now read all available arrival files of carriers
readallarrival(inputp, [aaarrivals, deltaarrivals, jbarrivals, unarrivals]).{
    show(stdout, "text/plain", skim(it))    pageddf(it, 1, 20, 5)
    them
}
# What is the most frequent tail_number? airport?
readallarrival(inputp, [aaarrivals, deltaarrivals, jbarrivals, unarrivals]).{
    groupby(it, :tail_number)           groupby(it, :origin_airport)
    combine(it, nrow)                   combine(it, nrow)
    sort(it, :nrow, rev = true)         sort(it, :nrow, rev = true)
    them
}

readallarrival(inputp, [aaarrivals, deltaarrivals, jbarrivals, unarrivals]).{
    groupby(it, :tail_number)               groupby(it, :origin_airport)
    combine(it, nrow)                       combine(it, nrow)
    sort(it, :nrow, rev = true)             sort(it, :nrow, rev = true)
    histogram(it.nrow, title = "Tails")     histogram(it.nrow, title = "Origins")
    them
}

readallarrival(inputp, [aaarrivals, deltaarrivals, jbarrivals, unarrivals]).{
    histogram(it.arrival_delay_minutes, title = "Arrival Delay, min")
}

readallarrival(inputp, [aaarrivals, deltaarrivals, jbarrivals, unarrivals]).{
    groupby(it, :carrier_code)
    [histogram(g.arrival_delay_minutes, title = "Arrival Delay, min") for g in it]
}

storearrivals(inputp, [aaarrivals, deltaarrivals, jbarrivals, unarrivals], "allarrivals.serial")

retrievearrivals(inputp, "allarrivals.serial").{
    Data(it) * Mark(:bar) * Facet(column=:carrier_code) * Encoding(
        x=field("arrival_delay_minutes:Q", bin=(;maxbins=50)),
        y="count()")
}

retrievearrivals(inputp, "allarrivals.serial").{
rainclouds(it.carrier_code, it.arrival_delay_minutes;
    # xlabel = "Carriers", ylabel = "Arrival delay, min",
    plot_boxplots = true, cloud_width=0.5, clouds=hist, hist_bins=100,
    # xticks = ([1, 2, 3, 4], string.(sort(unique(it.carrier_code)))),
    color = Makie.wong_colors()[indexin(it.carrier_code, sort(unique(it.carrier_code)))])
}

retrievearrivals(inputp, "allarrivals.serial").{
    describe(it)
    # string.(sort(unique(it.carrier_code)))
}


retrievearrivals(inputp, "allarrivals.serial").{
rainclouds(it.carrier_code, it.arrival_delay_minutes;
    xlabel = "Carriers", ylabel = "Arrival delay, min",
    plot_boxplots = true, cloud_width=0.5, clouds=hist, hist_bins=100,
    xticks = ([1, 2, 3, 4], string.(sort(unique(it.carrier_code)))),
    color = Makie.wong_colors()[indexin(it.carrier_code, sort(unique(it.carrier_code)))])
}

# Before running, change the font string to a monospaced font found on your system
retrievearrivals(inputp, "allarrivals.serial").{
    arrivaldelayviol(it, plotp, "arrival_delay_minutes.png"; font = "TheSansMono-W5Regular")
}

exit()
