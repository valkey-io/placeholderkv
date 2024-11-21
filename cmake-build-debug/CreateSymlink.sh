#!/bin/bash                                                 
if [ -z ${DESTDIR} ]; then                                 
    # Script is called during 'make install'                
    PREFIX=/usr/local/bin                      
else                                                        
    # Script is called during 'make package'                
    PREFIX=${DESTDIR}/bin 
fi                                                          
cd $PREFIX                                                 
ln -sf $1 $2
