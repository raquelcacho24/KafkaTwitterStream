package com.upm.etsit.raquel.tfg;


import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;

public class KafkaStream {
	
	public static void main(String[] args) {
		/*if (args.length < 5) {
			System.err.println("Usage: KafkaStream <kafka brokers> <topic> <Consumer Key> <Consumer Secret> <Access Token> <Access Token Secret>");
		    System.exit(1);
		}*/
		
		final String topic = "twitterdata";
		
		
		String consumerKey= "6Q57XQY3IyTGGb55PoXep1FTW";
		String consumerSecret= "GCXkzL8a33UwVIJftIcZeFtOnRxohS7SiFro8XkKuDrwb2lnJT";
		String token= "375645270-NNoRpUXIBCTdeveSfnJnB4W74rBVPnFwS4qKwnEJ";
		String secret= "HVle7dRaKKPUGBobmC6yxxpwAQxicGQeSy9WC2i3VdczE";
		
		
		
		
		
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "com.upm.etsit.raquel.tfg.TweetSerializer");
		props.put("metadata.broker.list","localhost:9092");
		props.put("serializer.class","kafka.serializer.StringEncoder");
		
		
		final KafkaProducer<String, Tweet> kafkaProducer = new KafkaProducer<String, Tweet>(props);
		
		
		TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
		twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
		twitterStream.setOAuthAccessToken(new AccessToken(token, secret));
		StatusListener listener = new StatusListener() {
			private int count = 0;

			public void onException(Exception e) {
				e.printStackTrace();
			}

			public void onDeletionNotice(StatusDeletionNotice arg0) {
			} 

			public void onScrubGeo(long arg0, long arg1) {
			}

			public void onStallWarning(StallWarning arg0) {
			}

			public void onStatus(Status status) {
				
				Tweet currentTweet;
				
				if(status.getPlace()!=null){
				
					currentTweet = new Tweet(status.getCreatedAt(),status.getUser().getName(), status.getText(),status.getRetweetCount(), status.getPlace().getCountry() );
				
				}else{
					currentTweet = new Tweet(status.getCreatedAt(),status.getUser().getName(), status.getText(),status.getRetweetCount(), "Sin Localización" );

				}
				
				System.out.println(currentTweet.getDate());
				System.out.println(currentTweet.getName());
				System.out.println(currentTweet.getText());
				//System.out.println(currentTweet.getHashtag());
				System.out.println(currentTweet.getRetweets());
				ProducerRecord<String, Tweet> record = new ProducerRecord<String, Tweet>(topic, Integer.toString(count), currentTweet);
				kafkaProducer.send(record);
				count++;
			}

			public void onTrackLimitationNotice(int arg0) {
			}
		};
		twitterStream.addListener(listener);
		FilterQuery query = new FilterQuery();
		//Ahora esta puesto en ingles
		query.language("en");
		query.track("a");
		twitterStream.filter(query);
	}
}