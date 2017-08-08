CC = gcc
CFLAGS = -std=gnu99 -Wall

OFP_HOME = /root/ofp
INCLUDE = -I$(OFP_HOME)/include -I$(OFP_HOME)/scripts/check-odp/new-build/include
LIB = -Wl,--whole-archive -L$(OFP_HOME)/lib -L$(OFP_HOME)/scripts/check-odp/new-build/lib $(OFP_HOME)/lib/.libs/libofp.a -lofp -lodp-dpdk -lodphelper-linux -lpthread -Wl,--no-whole-archive

all : ofpcp

%.o : %.c
	$(CC) $(CFLAGS) $(INCLUDE) -o $@ -c $<

ofpcp : ofpcp.o
	$(CC) $(CFLAGS) $(LIB) -o $@ $^ 

clean:
	rm -f *.o
	rm -f ofpcp

