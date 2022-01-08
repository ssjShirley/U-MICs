# U-MICs
YouTube Music Index of Chrismas songs/PlayLists  

The reason why we start this project is due to we cannot find satisfied Chrismas playlist from Youtube search result. No matter what kay words we entered, we always see the samilar result and recommend still same. So we want to explore the basic algorithm of Youtube and build a new search system(which more focus on word description) to help users can find more suitable playlist for them.

In This project we have 2 main parts:
* part 1:  Chrimas song analysis
  >In this part we choosed almost 4000 videos which their channel id shows in the first 3 pages when we tried to search chrimas and we monitored their views, like and comment from 2021 - 12 - 22 to 2021 - 12 -26(pacific time). The code of this can be found in  /U-MICs/src/youtube_api.ipynb.

  > Due to the time we start was very close to Chrimas and we want to Mointor as more days as we can, so the data we got from youtube_api.ipynb is not clearning data and had a large size. So we used Spark to ELT database to return the result which can safely running in pandas and Numpy.

  > Then we did some brief visualization and analysization on up data and we feel get the conclusion that the search system is more focus on the numerical part such as history views, channel followers but pay less focus on the word description part. You can see more details via /U-MICs/src/X_mas_analysis.ipynb. And the prediction by Machine Modes still can prove this result. (U-MICs/src/xMas_prediction.ipynb)

 * part 2: Building a search system.

      >After  the first step, we know the basic algorithn of Youtube System so want to build a new search system which more focus on video tags and user can have more filters, so they can have more appropriate result.

      >we Update the youtube_api.ipynb which can save and update Youtube video information in AWS RDS database. And I still Uploda this dataset to AWS S3 bucket so we can use mySQL or sql function in AWS to select the video.

* For the Future
    > 1. we are working on web vision for our search system by django which user who without computer knowledge still can using this system.

    >2. we try to build a recommend system basce on the user's listening behavior.