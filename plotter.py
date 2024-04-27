import matplotlib.pyplot as plt
import numpy as np

class Plotter:

    def plot(self, x_names: tuple, bars: dict, y_label: str, title: str, leg_loc: str, y_scale: tuple, plot_name: str):

        x = np.arange(len(x_names))
        width = 0.25
        multiplier = 0

        fig, ax = plt.subplots(figsize=(20,10))

        for attribute, measurement in bars.items():
            offset = width * multiplier
            rects = ax.bar(x + offset, measurement, width, label=attribute)
            ax.bar_label(rects, padding=3)
            multiplier += 1

        ax.set_ylabel(y_label)
        ax.set_title(title)
        ax.set_xticks(x + width, x_names)
        ax.legend(loc=leg_loc, ncols=len(bars))
        ax.set_ylim(y_scale[0], y_scale[1])

        plt.savefig(f"/data/results/plots/{plot_name}")