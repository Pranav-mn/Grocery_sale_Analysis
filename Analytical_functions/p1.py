import matplotlib.pyplot as plt
def visualize(tablename,x,y,save_path):
    tablename = tablename.toPandas()
    x = tablename[x]
    y = tablename[y]
    plt.figure(figsize=(25, 15))
    plt.plot(x,y)
    plt.title('Number sales in each Hour')
    plt.xlabel('Time in Hours')
    plt.ylabel('Count')
    # plt.show()
    plt.savefig(save_path, format='pdf')
