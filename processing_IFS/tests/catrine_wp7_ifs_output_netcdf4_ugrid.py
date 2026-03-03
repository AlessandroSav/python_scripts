import cfgrib
import numpy as np
from netCDF4 import Dataset
import subprocess
import numpy as np
import os
import sys
import pandas as pd
import datetime

# IFS 137 level hybrid coefficients

a= np.array([ 2.000365, 3.102241, 4.666084, 6.827977, 9.746966, 13.605424, 18.608931, 24.985718, 32.985710, 42.879242, 54.955463, 69.520576, 86.895882, 107.415741, 131.425507, 159.279404, 191.338562, 227.968948, 269.539581, 316.420746, 368.982361, 427.592499, 492.616028, 564.413452, 643.339905, 729.744141, 823.967834, 926.344910, 1037.201172, 1156.853638, 1285.610352, 1423.770142, 1571.622925, 1729.448975, 1897.519287, 2076.095947, 2265.431641, 2465.770508, 2677.348145, 2900.391357, 3135.119385, 3381.743652, 3640.468262, 3911.490479, 4194.930664, 4490.817383, 4799.149414, 5119.895020, 5452.990723, 5798.344727, 6156.074219, 6526.946777, 6911.870605, 7311.869141, 7727.412109, 8159.354004, 8608.525391, 9076.400391, 9562.682617, 10065.978516, 10584.631836, 11116.662109, 11660.067383, 12211.547852, 12766.873047, 13324.668945, 13881.331055, 14432.139648, 14975.615234, 15508.256836, 16026.115234, 16527.322266, 17008.789062, 17467.613281, 17901.621094, 18308.433594, 18685.718750, 19031.289062, 19343.511719, 19620.042969, 19859.390625, 20059.931641, 20219.664062, 20337.863281, 20412.308594, 20442.078125, 20425.718750, 20361.816406, 20249.511719, 20087.085938, 19874.025391, 19608.572266, 19290.226562, 18917.460938, 18489.707031, 18006.925781, 17471.839844, 16888.687500, 16262.046875, 15596.695312, 14898.453125, 14173.324219, 13427.769531, 12668.257812, 11901.339844, 11133.304688, 10370.175781, 9617.515625, 8880.453125, 8163.375000, 7470.343750, 6804.421875, 6168.531250, 5564.382812, 4993.796875, 4457.375000, 3955.960938, 3489.234375, 3057.265625, 2659.140625, 2294.242188, 1961.500000, 1659.476562, 1387.546875, 1143.250000, 926.507812, 734.992188, 568.062500, 424.414062, 302.476562, 202.484375, 122.101562, 62.781250, 22.835938, 3.757813, 0.000000, 0.000000 ])

b = np.array([ 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000007, 0.000024, 0.000059, 0.000112, 0.000199, 0.000340, 0.000562, 0.000890, 0.001353, 0.001992, 0.002857, 0.003971, 0.005378, 0.007133, 0.009261, 0.011806, 0.014816, 0.018318, 0.022355, 0.026964, 0.032176, 0.038026, 0.044548, 0.051773, 0.059728, 0.068448, 0.077958, 0.088286, 0.099462, 0.111505, 0.124448, 0.138313, 0.153125, 0.168910, 0.185689, 0.203491, 0.222333, 0.242244, 0.263242, 0.285354, 0.308598, 0.332939, 0.358254, 0.384363, 0.411125, 0.438391, 0.466003, 0.493800, 0.521619, 0.549301, 0.576692, 0.603648, 0.630036, 0.655736, 0.680643, 0.704669, 0.727739, 0.749797, 0.770798, 0.790717, 0.809536, 0.827256, 0.843881, 0.859432, 0.873929, 0.887408, 0.899900, 0.911448, 0.922096, 0.931881, 0.940860, 0.949064, 0.956550, 0.963352, 0.969513, 0.975078, 0.980072, 0.984542, 0.988500, 0.991984, 0.995003, 0.997630, 1.000000 ])

def retrieve_2d_ifs_output_grib_file(datapath,paramId,paramName,expver,resol,dates):
    """ Retrieve IFS 2D model output (sfc fields)
    """
    expclass="rd"
    stream="oper"
    exptype="fc"
    lstep="0/3/6/9/12/15/18/21/24"
    ltime=0
    levtype="sfc"    
    fgrib="ifs_2d_%s_%s_%s.grib" % (paramName,expver,dates)
    fgrib=os.path.join(datapath,fgrib)

    if not os.path.exists(fgrib):

        cmd=f'''
cat << EOF > mars_cmd_2d
retrieve,
class   =  {expclass} ,
time     = {ltime},
date     = {dates},
stream   =  {stream} , 
grid     =  {grid} , 
resol    =  {resol} , 
step     =  {lstep} , 
levtype  =  {levtype} , 
expver   =  {expver} , 
type     =  {exptype} ,
param    =  {paramId},
target   = "{fgrib}"
EOF
mars mars_cmd_2d
'''
        subprocess.check_output(cmd,shell=True)
        print(">>>  grib output file: %s " % fgrib)

    return(fgrib)
    
def retrieve_3d_ifs_output_grib_file(datapath,paramId,paramName,expver,resol,dates):
    """ Retrieve IFS 3D model output on hybrid model levels
    """
    expclass="rd"
    stream="oper"
    exptype="fc"
    lstep="0/3/6/9/12/15/18/21/24"
    ltime=0
    levtype='ml'    
    llevelist="1/to/137/by/1"
    fgrib="ifs_3d_%s_%s_%s.grib" % (paramName,expver,dates)
    fgrib_lnsp=fgrib.replace(paramName,'lnsp')
    fgrib=os.path.join(datapath,fgrib)
    fgrib_lnsp=os.path.join(datapath,fgrib_lnsp)    
    if not os.path.exists(fgrib):

        cmd=f'''
cat << EOF > mars_cmd_3d
retrieve,
class   =  {expclass} ,
time     = {ltime},
date     = {dates},
stream   =  {stream} , 
grid     =  {grid} , 
resol    =  {resol} , 
step     =  {lstep} , 
levtype  =  {levtype} , 
expver   =  {expver} , 
type     =  {exptype} ,
param    =  {paramId},
levelist =  {llevelist},
target   = "{fgrib}"
EOF
mars mars_cmd_3d
'''
        subprocess.check_output(cmd,shell=True)
        print(">>>  grib output file: %s " % fgrib)

    if not os.path.exists(fgrib_lnsp):
        
        cmd2=f'''
cat << EOF > mars_cmd_lnsp
retrieve,
class   =  {expclass} ,
time     = {ltime},
date     = {dates},
stream   =  {stream} , 
grid     =  {grid} , 
resol    =  {resol} , 
step     =  {lstep} , 
levtype  =  {levtype} , 
expver   =  {expver} , 
type     =  {exptype} ,
param    =  lnsp,
levelist =  1,
target   = "{fgrib_lnsp}"
EOF
mars mars_cmd_lnsp
'''
        subprocess.check_output(cmd2,shell=True)
        print(">>>  grib output file: %s " % fgrib_lnsp)        

    return(fgrib)

def convert_2d_output_grib_to_netcdf(paramName,grib_file, output_netcdf):
    # Open the GRIB file using cfgrib
    ds = cfgrib.open_dataset(grib_file)

    # Create a NetCDF4 file
    if os.path.exists(output_netcdf):
        os.remove(output_netcdf)
    nc = Dataset(output_netcdf, mode='w', format='NETCDF4')

    # Define dimensions from the GRIB file (for UGRID you would likely want to extract points)
    n_points = ds.sizes['values']  # Adjust according to the GRIB structure
    nc.createDimension('nodes', n_points)
    nstep = ds.sizes['step']  # Adjust according to the GRIB structure    
    nc.createDimension('step', nstep)

    # Define the topology (UGRID specific)
    # We define a single unstructured grid 'mesh'
    mesh = nc.createVariable('mesh', 'i4', ())
    mesh.cf_role = "mesh_topology"
    mesh.topology_dimension = 2  
    mesh.node_coordinates = "lon lat"

    # Write latitude and longitude (these are the node coordinates)
    latitudes = nc.createVariable('lat', np.float32, ('nodes',))
    longitudes = nc.createVariable('lon', np.float32, ('nodes',))

    # Write other coordinates and auxiliary variables
    steps = nc.createVariable('step', np.float32, ('step',))
    dates = nc.createVariable('date', int, ('step',))
    hours = nc.createVariable('hour', int, ('step',))

    latitudes.units = "degrees_north"
    longitudes.units = "degrees_east"
    steps.units="simulation hours since 00 UTC"
    dates.units="valid date for each simulated step"
    hours.units="valid time of day in hours for each simulated step"    
    
    latitudes[:] = ds.latitude.values.astype(np.float32)
    longitudes[:] = ds.longitude.values.astype(np.float32)
    steps[:] = 1e-9*ds.step.values/(3600.)
    dates[:] = pd.to_datetime(ds.valid_time.values).strftime('%Y%m%d').astype(int)
    hours[:] = pd.to_datetime(ds.valid_time.values).strftime('%H').astype(int)
    
    # Write the actual data in single precision (32-bit floats)
    for var_name in ds.variables:
        if var_name not in ['latitude', 'longitude', 'valid_time', 'time','step','number','surface']:
            print("*** GET variable from grib file: ", var_name)
            var = ds[var_name]
            out_var = nc.createVariable(var_name, np.float32, ('step','nodes',), zlib=True)
            out_var[:,:] = var.values.astype(np.float32)
            out_var.units = var.units

    # Close the NetCDF file
    nc.close()

def convert_3d_output_grib_to_netcdf(paramName,grib_file, output_netcdf):
    # Open the GRIB file using cfgrib
    ds = cfgrib.open_dataset(grib_file)

    lnsp_grib_file=grib_file.replace(paramName,'lnsp')
    lnsp_ds=cfgrib.open_dataset(lnsp_grib_file)

    # Create a NetCDF4 file
    if os.path.exists(output_netcdf):
        os.remove(output_netcdf)
    nc = Dataset(output_netcdf, mode='w', format='NETCDF4')

    # Define dimensions from the GRIB file (for UGRID you would likely want to extract points)
    n_points = ds.sizes['values']  # Adjust according to the GRIB structure
    nc.createDimension('nodes', n_points)
    nstep = ds.sizes['step']  # Adjust according to the GRIB structure    
    nc.createDimension('step', nstep)
    nlev = ds.sizes['hybrid']  # Adjust according to the GRIB structure    
    nc.createDimension('hlevel', nlev)    

    # Define the topology (UGRID specific)
    # We define a single unstructured grid 'mesh'
    mesh = nc.createVariable('mesh', 'i4', ())
    mesh.cf_role = "mesh_topology"
    mesh.topology_dimension = 2  
    mesh.node_coordinates = "lon lat"

    # Write latitude and longitude (these are the node coordinates)
    latitudes = nc.createVariable('lat', np.float32, ('nodes',))
    longitudes = nc.createVariable('lon', np.float32, ('nodes',))

    # Write other coordinates and auxiliary variables
    steps = nc.createVariable('step', np.float32, ('step',))
    levels = nc.createVariable('hlevel', np.float32, ('hlevel',))
    dates = nc.createVariable('date', int, ('step',))
    hours = nc.createVariable('hour', int, ('step',))

    ap = nc.createVariable('ap','f4',('hlevel',))
    bp = nc.createVariable('bp','f4',('hlevel',))
    
    latitudes.units = "degrees_north"
    longitudes.units = "degrees_east"
    steps.units="simulation hours since 00 UTC"
    levels.units="vertical level starting from top of atmosphere"
    dates.units="valid date for each simulated step"
    hours.units="valid time of day in hours for each simulated step"    
    ap.long_name = 'IFS %d model level definition A(N) coefficients' % 137
    bp.long_name = 'IFS %d model level definition B(N) coefficients' % 137
    ap.units = 'Pa'
    bp.units = '1'
    
    latitudes[:] = ds.latitude.values.astype(np.float32)
    longitudes[:] = ds.longitude.values.astype(np.float32)
    steps[:] = 1e-9*ds.step.values/(3600.)
    levels[:] = ds.hybrid.astype(np.float32)    
    dates[:] = pd.to_datetime(ds.valid_time.values).strftime('%Y%m%d').astype(int)
    hours[:] = pd.to_datetime(ds.valid_time.values).strftime('%H').astype(int)
    ap[:] = a
    bp[:] = b
    
    # Write the actual data in single precision (32-bit floats)
    for var_name in ds.variables:
        if var_name not in ['latitude', 'longitude', 'valid_time', 'time','step','hybrid']:
            print("*** GET variable from grib file: ", var_name)
            var = ds[var_name]
            out_var = nc.createVariable(var_name, np.float32, ('step','hlevel','nodes',), zlib=True)
            out_var[:,:,:] = var.values.astype(np.float32)
            out_var.units = var.units

    Psurf_var = nc.createVariable('Psurf', np.float32, ('step','nodes',), zlib=True)
    Psurf_var[:,:] = np.exp(lnsp_ds['lnsp'].values).astype(np.float32)
    Psurf_var.long_name = "Surface pressure"    
    Psurf_var.units = 'Pa'
    
    # Close the NetCDF file
    nc.close()


if __name__ == '__main__' :
  
    pythonscr=sys.argv[0]
    paramName=sys.argv[1]
    yyyymmdd1=sys.argv[2]
    yyyymmdd2=sys.argv[3]

    print(paramName)
    print(yyyymmdd1)
    print(yyyymmdd2)
    
    datapath="/scratch/paaa/IFS/cabauw/"    

    ## WP7 IFS simulation for first round of model inter-comparison 
    # Here you should put the name of the experimetn as save in the scratch folder above. 
    if "2022" in yyyymmdd1:
        expver="inii"
    elif "2016" in yyyymmdd1 or "2017" in yyyymmdd1:
        expver="iiya"
        
    resol="399"
    grid="O400"

    ## Parameters to extract:
    fieldis3d=False
    fieldis2d=False
    if paramName.lower() == "co2":
        ###  CO2 mixing ratio [kg/kg]
        paramId="61.210"
        fieldis3d = True
    elif  paramName.lower() == "co2apf":
        ### Anthropogenic CO2 mixing ratio [kg/kg]
        paramId="158.212"
        fieldis3d = True        
    elif  paramName.lower() == "sf6":
        ### SF6 mixing ratio [kg/kg]
        paramId="21.212"
        fieldis3d = True        
    elif paramName.lower() == "rn222":
        ###  Radon mixing ratio [kg/kg]                
        paramId="181.210"
        fieldis3d = True
    elif paramName.lower() == "co":
        ###  CO mixing ratio [kg/kg]        
        paramId="123.210"
        fieldis3d = True        
    elif paramName.lower() == "ch4":
        ###  CH4 mixing ratio [kg/kg]
        paramId="62.210"
        fieldis3d = True        
    elif paramName.lower() == "q":
        ###  humidity mixing ratio [kg/kg]
        paramId="133"
        fieldis3d = True
    elif paramName.lower() == "sp":
        ###  Surface pressure [Pa]
        paramId="134"
        fieldis2d = True
    elif paramName.lower() == "blh":
        ###  PBL height [m]
        paramId="159"
        fieldis2d = True
    elif paramName.lower() == "ThermalTropopause":
        ###  thermal tropopause height [m]        
        paramId="67.216"
        fieldis2d = True
#    elif paramName.lower() == "PVTropopause":
#    !!! Need a new function to extract the fields on PV levels!!!
#        paramId=""
#        fieldis2d = False
#        fieldisonpv = True
    elif paramName.lower() == "VINfluxCO2":
        ### Vertically integrated northward flux of CO2
        paramId="4.216"
        fieldis2d = True
    elif paramName.lower() == "VIEfluxCO2":
        ### Vertically integrated eastward flux of CO2    
        paramId="1.216"
        fieldis2d = True
    elif paramName.lower() == "VINfluxCO2APF":
        ### Vertically integrated northward flux of anthropogenic CO2
        paramId="25.216"
        fieldis2d = True
    elif paramName.lower() == "VIEfluxCO2APF":
        ### Vertically integrated eastward flux of anthropogenic CO2    
        paramId="32.216"
        fieldis2d = True
    elif paramName.lower() == "VINfluxSF6":
        ### Vertically integrated northward flux of SF6
        paramId="26.216"
        fieldis2d = True
    elif paramName.lower() == "VIEfluxSF6":
        ### Vertically integrated eastward flux of SF6    
        paramId="33.216"
        fieldis2d = True
    elif paramName.lower() == "VINfluxRn222":
        ### Vertically integrated northward flux of Rn222
        paramId="28.216"
        fieldis2d = True
    elif paramName.lower() == "VIEfluxRn222":
        ### Vertically integrated eastward flux of Rn222
        paramId="31.216"
        fieldis2d = True        
        
    date1 = datetime.datetime.strptime(str(yyyymmdd1), '%Y%m%d')
    date2 = datetime.datetime.strptime(str(yyyymmdd2), '%Y%m%d')      

    date=date1
    while date <= date2:
        yyyymmdd=date.strftime('%Y%m%d')
        if fieldis3d:
            fgrib=retrieve_3d_ifs_output_grib_file(datapath,paramId,paramName.lower(),expver,resol,yyyymmdd)
            fnc=fgrib.replace('grib','nc')
            convert_3d_output_grib_to_netcdf(paramName.lower(),fgrib, fnc)
            print(">>>> 3D NC output file: %s" % fnc)
        elif fieldis2d:
            fgrib=retrieve_2d_ifs_output_grib_file(datapath,paramId,paramName.lower(),expver,resol,yyyymmdd)
            fnc=fgrib.replace('grib','nc')
            convert_2d_output_grib_to_netcdf(paramName.lower(),fgrib, fnc)
            print(">>>> 2D NC output file: %s" % fnc)

            
        date += datetime.timedelta(days=1)    
