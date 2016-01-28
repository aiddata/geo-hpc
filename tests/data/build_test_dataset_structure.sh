#!/bin/bash

base=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

cd $base

mkdir -p datasets/{dataset_01,dataset_02,dataset_03,dataset_04}

# temporally invariant
cp SRTM_500m_clip.tif datasets/dataset_01/temporally_invariant.tif

# year
cp SRTM_500m_clip.tif datasets/dataset_02/year_2000.tif
cp SRTM_500m_clip.tif datasets/dataset_02/year_2001.tif
cp SRTM_500m_clip.tif datasets/dataset_02/year_2002.tif
cp SRTM_500m_clip.tif datasets/dataset_02/year_2003.tif
cp SRTM_500m_clip.tif datasets/dataset_02/year_2004.tif

# year month
mkdir datasets/dataset_03/{2000,2001}
cp SRTM_500m_clip.tif datasets/dataset_03/2000/year_month_2000_01.tif
cp SRTM_500m_clip.tif datasets/dataset_03/2000/year_month_2000_02.tif
cp SRTM_500m_clip.tif datasets/dataset_03/2000/year_month_2000_03.tif
cp SRTM_500m_clip.tif datasets/dataset_03/2001/year_month_2001_01.tif
cp SRTM_500m_clip.tif datasets/dataset_03/2001/year_month_2001_02.tif
cp SRTM_500m_clip.tif datasets/dataset_03/2001/year_month_2001_03.tif

# year day
mkdir datasets/dataset_04/{2000,2001}
cp SRTM_500m_clip.tif datasets/dataset_04/2000/year_day_2000_008.tif
cp SRTM_500m_clip.tif datasets/dataset_04/2000/year_day_2000_105.tif
cp SRTM_500m_clip.tif datasets/dataset_04/2000/year_day_2000_355.tif
cp SRTM_500m_clip.tif datasets/dataset_04/2001/year_day_2001_026.tif
cp SRTM_500m_clip.tif datasets/dataset_04/2001/year_day_2001_237.tif
cp SRTM_500m_clip.tif datasets/dataset_04/2001/year_day_2001_300.tif




