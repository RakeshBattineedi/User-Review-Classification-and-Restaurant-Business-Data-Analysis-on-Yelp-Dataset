# User-Review-Classification-and-Restaurant-Business-Data-Analysis-on-Yelp-Dataset

Problem Formulation:

The food industry is one of the most dynamic industries today, and it must keep evolving along with consumer demands. They must keep up with customer trends and preferences, deliver more enjoyable culinary experiences, enhance marketing and promotions and much more. Most of the food or cuisine trends are identified around the buzz words in the industry and through advertising via various platforms. Although this is crucial to set up a restaurant business in some locality, it is important to consider various factors like user reviews, popular cuisines around the city, customer satisfaction factor finding a holistic view.

The aim of our project is to classify user reviews into three categories: Positive, negative and neutral. This analysis will enable us to predict a customer’s satisfaction through his review. In doing so, we also want to analyse which algorithm between Naive Bayes and Logistic Regression performs the task more efficiently. The secondary goal of the project is to perform various analyses on the Yelp Business dataset and draw various insights from these analyses.

Yelp is one of the most successful applications that connect people with local businesses. Yelp had a monthly average of 37 million unique visitors who visited Yelp via the Yelp app and 77 million unique visitors who visited Yelp via mobile web in Q2 2019. * Also, yelpers (users of yelp) have written more than 192 million reviews by the end of Q2 2019. With so much data lying around, the amount of insights we can gain is tremendous. Many people can benefit from this analysis ranging from existing business owners to new budding entrepreneurs in the food industry.

Part-I: Methodology to Solve the Classification of User Reviews:

The algorithms/techniques/models used in this project:

● Extract the JSON format to dataframe, pre-process, and clean the data related to review.json from Yelp[1] academic data set.

● For user review prediction we are using tokenizer for making words and we are removing the stop words from the review text of the user.

● HashingTF and CountVectorizer are used to generate the term frequency vectors which are input features for machine learning algorithms.

● IDF is an Estimator which scales each feature and it down-weighs features which appear frequently in a corpus.

● Split the data into training and testing set.

● Use the Logistic Regression and Naive Bayes algorithms to classify the user review into three categories as positive(rating>= 3.5), neutral(3.5>rating>=2.5), negative (2.5>rating>=0) on test data set and verify the accuracy how good the algorithms are able to classify the user review.

The framework used in this project:

● We implemented the above using the SPARK framework in scala. We used Machine learning libraries
