from NiceTab import NiceTab
import pyarrow.csv as pcsv

# Wrap Pyarrow's csv reader in NiceTab() to initialize.
data = NiceTab(pcsv.read_csv('world_data.psv', parse_options=pcsv.ParseOptions(delimiter='|')))
what_we_got = len(data)
print(f"∙ Got {what_we_got} rows of data. Read these columns:")
print(data.tab.column_names)

#But we'll only use these columns. "entcari"=national territorial CO2 emissions; "npopuli"=population.
data = data.select(['country', 'entcari','npopuli'])

print("∙ Here's a sample of the columns we'll be using:")
print(data.tab.to_pandas())


#This lambda takes in a NiceTab and returns a NiceVec giving which observations have nonzero CO2 data.  
has_co2_data = lambda data_in: data_in.entcari>0
data = data.filter(has_co2_data)

#For something this simple, outside of demo code, one could use the condition directly.
#data = data.filter(data.entcari>0)

print(f"∙ Only {len(data)} entries ({100*len(data)/(what_we_got+0.0):.1f}%) have nonzero CO2 measurements; we threw out the rest.")

#Add a column with tons of CO2 per capita.
data.co2_per_cap = data.entcari / data.npopuli * 1000000


#Query some statistics. We get a Pandas data frame, and in all cases here
#we only want the first entry in the first column.
most_co2=data.Q("entcari", aggregation='max').iloc[0,0]
avg_co2 =data.Q("entcari", aggregation='avg', weight='npopuli', where=data.npopuli>0).iloc[0,0]
most_co2_ctry = data.Q("country", where=(data.entcari==most_co2)).iloc[0,0]

print()
print(f"∙ In 2020, {most_co2_ctry} has the largest national territorial CO2 emissions, {most_co2:,.0f}, compared to the (population-weighted) average of {avg_co2:,.2f}.")
print(f"(Numbers are in millions of tons, so 10,000=10 billion tons.)")

#Now the per-population stats, a very similar calculation.
most_co2_per_cap =data.Q("co2_per_cap", aggregation='max').iloc[0,0]
avg_co2_per_cap =data.Q("co2_per_cap", aggregation='avg', weight='npopuli', where=data.npopuli>0).iloc[0,0]
most_cpc_ctry = data.Q("country", where=(data.co2_per_cap==most_co2_per_cap)).iloc[0,0]
pop_of_most_cpc_ctry = data.Q("npopuli", where=(data.country==most_cpc_ctry)).iloc[0,0]

print()
print(f"∙ {most_cpc_ctry}, population {pop_of_most_cpc_ctry:,.0f}, has the most CO2 emission per capita, at {most_co2_per_cap:.2f} tons per capita, versus a population-weighted average of {avg_co2_per_cap:.4f} tons.")

# Use base Pyarrow to save the partial data we selected and generated.
print()
print(f"∙ Writing 'data.out' to disk with the data we used or generated")
pcsv.write_csv(data.tab, 'data.out')
