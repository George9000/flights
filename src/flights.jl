module flights
using CSV, DataFrames, CategoricalArrays, Dates, Missings, Serialization
using UnicodePlots, Statistics, StatsBase, FreqTables, Chain
using AnalysisUtils, MethodChains
import TableTransforms: StdNames
using CairoMakie, ColorSchemes
export readarrival, readallarrival, storearrivals, retrievearrivals
export arrivaldelayviol

"""
    readarrival(inputpath, filename)

Read in a flight arrivals and process it to a DataFrame.
"""
function readarrival(inputpath, filename; version=1)
    input = joinpath(inputpath, filename)
    if version == 1
        CSV.read(input, DataFrame; header = 8, footerskip = 2)
    elseif version == 2
        CSV.read(input, DataFrame; header = 8, footerskip = 2,
            dateformat = Dict(
                2 => dateformat"m/d/y",
                6 => dateformat"H:M",
                7 => dateformat"H:M",
                11 => dateformat"H:M"))
    elseif version == 3
        CSV.read(input, DataFrame; header = 8, footerskip = 2,
            types = Dict(
                2 => Date,
                6 => Time,
                7 => Time,
                11 => Time),
            dateformat = Dict(
                2 => dateformat"m/d/y",
                6 => dateformat"H:M",
                7 => dateformat"H:M",
                11 => dateformat"H:M"))
    else
        df = CSV.read(input, DataFrame; dateformat = "m/d/y", header = 8, footerskip = 2)
        transform!(df, [7, 11] .=> ByRow(str -> replace(str, "24:00" => "00:00")), renamecols = false)
        transform!(df, [6, 7, 11] .=> ByRow(str -> Time(str, dateformat"H:M")), renamecols = false)
        return df
    end
end

"""
    readallarrival(inputpath, filenamesvector)

Read in a set of arrivals flight data files and concatenate them into one dataframe.
"""
function readallarrival(inputpath, filenamesvector)
    resultdf = DataFrame()
    for f in filenamesvector
        input = joinpath(inputpath, f)
        @mc CSV.read(input, DataFrame; dateformat = "m/d/y", header = 8, footerskip = 2).{
            transform!(it, [7, 11] .=> ByRow(str -> replace(str, "24:00" => "00:00")), renamecols = false)
            transform!(it, [6, 7, 11] .=> ByRow(str -> Time(str, dateformat"H:M")), renamecols = false)
            resultdf = vcat(it, resultdf)
        }
    end
    return resultdf |> StdNames(:snake)
end

"""
    storearrivals(inputpath, infilename, outfilename)

Store all arrivals in a serial file.
"""
function storearrivals(inputpath, filenamesvector, outfilename)
    return serialize(joinpath(inputpath, outfilename), readallarrival(inputpath, filenamesvector))
end

"""
    retrievearrivals(inputpath, filename)

Deserialize arrivals serial file.
"""
function retrievearrivals(inputpath, filename)
    return deserialize(joinpath(inputpath, filename))
end

"""
    arrivaldelayviol(df, ppath, pname; font = "TheSansMono-W5Regular")

Plot violin plots of distribution of arrival delay in minutes for each carrier.
"""
function arrivaldelayviol(df, ppath, pname; font = "TheSansMono-W5Regular")
    categories = string.(sort(unique(df.carrier_code)))
    colors = categorical_colors(:Set1, length(categories))
    fig = Figure(size = (900, 600), fontsize = 14, fonts = (; regular = font))
    ax = Axis(fig[1, 1], xticks = (1:length(categories), categories))
    for (indx, f) in enumerate(categories)
        delaym = subset(df, :carrier_code => ByRow(==(f)))[:, :arrival_delay_minutes]
        carrier = fill(indx, length(delaym))
        carrier2 = carrier .+ rand(length(delaym)) .+ -0.5
        scatter!(ax, carrier2, delaym; color = (colors[indx], 0.25), markersize = 2)
        # violin!(ax, carrier, delaym; width = 0.35, color = (:grey, 0.9), strokecolor = colors[indx], show_median = true, mediancolor = :black)
        violin!(ax, carrier, delaym; width = 0.35, color = (colors[indx], 0.9), strokecolor = colors[indx], show_median = true, mediancolor = :black)
        ylims!(ax, -100, 100)
    end
    save(joinpath(ppath, pname), fig; px_per_unit = 3)
end


end # module flights
