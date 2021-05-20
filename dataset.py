import threading
from dataclasses import dataclass

@dataclass
class Box:
    x_1: float
    x_2: float
    y_1: float
    y_2: float

class Dataset:
    def __init__(self, df):
        self.df = df
        box = Box(x_1=self.df.iloc[:, 0].min(),
                  y_1=self.df.iloc[:, 1].min(),
                  x_2=self.df.iloc[:, 0].max(),
                  y_2=self.df.iloc[:, 1].max())
        self.median_boxes = [box]
        self.new_median_boxes = []
        self.lock = threading.Lock()

    def get_medians(self, thread_count, dim):
        with self.lock:
            median_boxes = self.median_boxes

        box = median_boxes[thread_count]

        t_df = self.df[(box.x_1 < self.df.iloc[:, 0]) &
                       (self.df.iloc[:, 0] <= box.x_2) &
                       (box.y_1 < self.df.iloc[:, 1]) &
                       (self.df.iloc[:, 1] <= box.y_2)]

        median = t_df.iloc[:, dim].median()
        median_1 = t_df[t_df.iloc[:, dim] > median].min()[dim]

        del t_df

        if dim:
            box_1 = Box(
                x_1=box.x_1,
                y_1=box.y_1,
                x_2=box.x_2,
                y_2=median
            )
            box_2 = Box(
                x_1=box.x_1,
                y_1=median_1,
                x_2=box.x_2,
                y_2=box.y_2
            )
        else:
            box_1 = Box(
                x_1=box.x_1,
                y_1=box.y_1,
                x_2=median,
                y_2=box.y_2
            )
            box_2 = Box(
                x_1=median_1,
                y_1=box.y_1,
                x_2=box.x_2,
                y_2=box.y_2
            )

        with self.lock:
            self.new_median_boxes.extend([box_1, box_2])

    def do_work(self, levels):
        dim = 0
        for level in range(levels):
            threads = []
            self.new_median_boxes = []
            for thread_count in range(pow(2, level)):
                thread = threading.Thread(target=self.get_medians, args=[thread_count, dim], daemon=True)
                thread.start()
                threads.append(thread)
            [thread.join() for thread in threads]
            self.median_boxes = self.new_median_boxes
            dim = 0 if dim else 1