module Utils

export
    build_dataframe,
    safe_convert,
    timestamp_to_seconds,
    safe_save,
    save_figs_in_dict,
    create_stat_matrix

using DataFrames
using Dates
using Plots
using StatsBase
using Distributions
using CSV

function build_dataframe(
    prj_inner_dict::Dict{String, Any},
    columns::Vector{String}
    )
    keys_list = collect(keys(prj_inner_dict));
    return_df_dict = Dict();

    for col in columns
        values = [prj_inner_dict[k][col] for k in keys_list];
        return_df_dict[col] = values;
    end

    return DataFrame(return_df_dict);
end

function safe_convert(T::Type, value, default)
    try
        return convert(T, value)
    catch
        return default
    end
end

function timestamp_to_seconds(timestamp::String)
    parts = split(timestamp, ":")
    if length(parts) != 3
        return missing
    end
    
    try
        hours = parse(Float64, parts[1])
        minutes = parse(Float64, parts[2])
        seconds = parse(Float64, parts[3])
        
        return hours * 3600 + minutes * 60 + seconds
    catch
        return missing
    end
end

function ensure_directory(path::String)
    if !isdir(path)
        mkpath(path)
    end
    return path
end

function safe_save(plot_obj, filepath::String)
    dir_path = dirname(filepath);
    
    ensure_directory(dir_path);
    
    savefig(plot_obj, filepath);
end

function safe_save_csv(df, filepath::String)
    dir_path = dirname(filepath);

    ensure_directory(dir_path);

    CSV.write(filepath, df);
end

function flatten_dict(dict::Dict; delimiter = "/", parent_key = "")
    items = [];
    for (key, value) in dict
        new_key = isempty(parent_key) ? string(key) : string(parent_key, delimiter, key);
        if value isa Dict
            append!(items, flatten_dict(value, delimiter = delimiter, parent_key = new_key))
        else
            push!(items, (new_key, value));
        end
    end

    return Dict(items);
end

function save_figs_in_dict(dict::Dict, output_dir::String)
    flattened_dict = flatten_dict(dict; parent_key = output_dir);
    for (key, value) in flattened_dict
        if value isa Function
            safe_save(value(), key);
        else
            safe_save(value, key);
        end
    end
end

function create_stat_matrix(df::DataFrame, column::String)
    observations = unique(df.session);
    plot = "category";
    if "code" in names(df)
        plot = "code";
    end
    col_vals = unique(df[:, plot]);

    count_matrix = zeros(Float64, length(col_vals), length(observations));

    for (i, col_val) in enumerate(col_vals)
        for (j, obs) in enumerate(observations)
            row = subset(
                df,
                Symbol(plot) => x -> x .== col_val,
                :session => x -> x .== obs
            )
            if nrow(row) !== 0
                count_matrix[i, j] = first(row[:, column]);
            end
        end
    end
    return count_matrix;
end

function spearman_test(v1::AbstractVector, v2::AbstractVector)
    n = length(v1);
    p = corspearman(v1, v2);
    t_stat = p * sqrt((n - 2) / (1 - p^2));
    p_value = 2 * (1 - cdf(TDist(n - 2), abs(t_stat)));
    return (correlation = p, p_value = p_value, t_statistic = t_stat);
end

end # module
