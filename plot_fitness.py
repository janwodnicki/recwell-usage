from process import process_updates
import pandas as pd
import plotly.express as px

def main():
    reduced = process_updates()
    reduced.max_count = pd.to_numeric(reduced.max_count)
    reduced.current_count = pd.to_numeric(reduced.current_count)
    reduced['capacity'] = reduced.current_count / reduced.max_count

    fitness = reduced[reduced.location.str.contains('Fitness')]
    fig = px.scatter(fitness, x='update_time', y='capacity', color='location')
    fig.update_yaxes(range=(0,2))
    fig.show()

if __name__ == '__main__':
    main()