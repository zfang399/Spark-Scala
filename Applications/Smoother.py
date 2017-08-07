import dateutil.parser as dparser
import matplotlib.pyplot as plt
import numpy as np
from pylab import *

def smooth(x,window_len):
	s=np.r_[2*x[0]-x[window_len-1::-1],x,2*x[-1]-x[-1:-window_len:-1]]
	w=np.hamming(window_len)
	y=np.convolve(w/w.sum(),s,mode='same')
	return y[window_len:-window_len+1]

x=np.genfromtxt("ExchangeRate.csv",
	dtype='object',
	delimiter=',',
	skip_header=1,
	usecols=(0),
	converters={0:dparser.parse})

originalTS=np.genfromtxt("ExchangeRate.csv",
	skip_header=1,
	dtype=None,
	delimiter=',',
	usecols=(1))

smoothedTS=smooth(originalTS,len(originalTS))
plt.step(x,originalTS,'co')
plt.step(x,smoothedTS)
plt.show()