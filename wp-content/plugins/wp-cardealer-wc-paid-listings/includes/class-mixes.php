<?php
/**
 * Order
 *
 * @package    wp-cardealer-wc-paid-listings
 * @author     Habq 
 * @license    GNU General Public License, version 3
 */
 
if ( ! defined( 'ABSPATH' ) ) {
	exit; // Exit if accessed directly
}

class WP_CarDealer_Wc_Paid_Listings_Mixes {

	public static function get_listing_package_products() {
		$query_args = array(
		   	'post_type' => 'product',
		   	'post_status' => 'publish',
			'posts_per_page'   => -1,
			'order'            => 'asc',
			'orderby'          => 'menu_order',
		   	'tax_query' => array(
		        array(
		            'taxonomy' => 'product_type',
		            'field'    => 'slug',
		            'terms'    => array('listing_package', 'listing_package_subscription'),
		        ),
		    ),
		);
		$posts = get_posts( $query_args );

		return $posts;
	}
	
	public static function create_user_package( $user_id, $product_id, $order_id ) {
		$package = wc_get_product( $product_id );

		if ( !$package->is_type( array('listing_package', 'listing_package_subscription') ) ) {
			return false;
		}

		$args = apply_filters( 'wp_cardealer_wc_paid_listings_create_user_package_data', array(
			'post_title' => $package->get_title(),
			'post_status' => 'publish',
			'post_type' => 'listing_package',
		), $user_id, $product_id, $order_id);

		$user_package_id = wp_insert_post( $args );
		if ( $user_package_id ) {
			// general metas
			$prefix = WP_CARDEALER_WC_PAID_LISTINGS_PREFIX;
			update_post_meta( $user_package_id, $prefix.'product_id', $product_id );
			update_post_meta( $user_package_id, $prefix.'order_id', $order_id );
			update_post_meta( $user_package_id, $prefix.'package_count', 0 );
			update_post_meta( $user_package_id, $prefix.'user_id', $user_id );
			update_post_meta( $user_package_id, $prefix.'package_type', 'listing_package' );

			// listing metas
			$feature_listings = get_post_meta($product_id, '_feature_listings', true );
			$duration_listings = get_post_meta($product_id, '_listings_duration', true );
			$limit_listings = get_post_meta($product_id, '_listings_limit', true );
			$subscription_type = get_post_meta($product_id, '_listing_package_subscription_type', true );

			if ( $feature_listings == 'yes' ) {
				update_post_meta( $user_package_id, $prefix.'feature_listings', 'on' );
			}
			update_post_meta( $user_package_id, $prefix.'listing_duration', $duration_listings );
			update_post_meta( $user_package_id, $prefix.'listing_limit', $limit_listings );
			update_post_meta( $user_package_id, $prefix.'subscription_type', $subscription_type );

			do_action('wp_cardealer_wc_paid_listings_create_user_package_meta', $user_package_id, $user_id, $product_id, $order_id);
		}

		return $user_package_id;
	}

	public static function approve_listing_with_package( $listing_id, $user_id, $user_package_id ) {
		if ( self::package_is_valid( $user_id, $user_package_id ) ) {

			$listing = array(
				'ID'            => $listing_id,
				'post_date'     => current_time( 'mysql' ),
				'post_date_gmt' => current_time( 'mysql', 1 )
			);
			$post_type = get_post_type( $listing_id );

			if ( $post_type === 'listing' ) {
				delete_post_meta( $listing_id, WP_CARDEALER_LISTING_PREFIX.'expiry_date' );

				$review_before = wp_cardealer_get_option( 'submission_requires_approval' );
				$post_status = 'publish';
				if ( $review_before == 'on' ) {
					$post_status = 'pending';
				}

				$listing['post_status'] = $post_status;
				
			}

			// Do update
			wp_update_post( $listing );
			update_post_meta( $listing_id, WP_CARDEALER_LISTING_PREFIX.'user_package_id', $user_package_id );
			self::increase_package_count( $user_id, $user_package_id );

			do_action('wp_cardealer_wc_paid_listings_approve_listing_with_package', $listing_id, $user_id, $user_package_id);
		}
	}

	public static function package_is_valid( $user_id, $user_package_id ) {
		$post = get_post($user_package_id);
		if ( empty($post) ) {
			return false;
		}
		$prefix = WP_CARDEALER_WC_PAID_LISTINGS_PREFIX;
		$package_user_id = get_post_meta($user_package_id, $prefix.'user_id', true);
		$package_count = get_post_meta($user_package_id, $prefix.'package_count', true);
		$listing_limit = get_post_meta($user_package_id, $prefix.'listing_limit', true);

		if ( ($package_user_id != $user_id) || ($package_count >= $listing_limit && $listing_limit != 0) ) {
			return false;
		}

		return true;
	}

	public static function increase_package_count( $user_id, $user_package_id ) {
		$prefix = WP_CARDEALER_WC_PAID_LISTINGS_PREFIX;
		$post = get_post($user_package_id);
		if ( empty($post) ) {
			return false;
		}
		$package_user_id = get_post_meta($user_package_id, $prefix.'user_id', true);
		
		if ( $package_user_id != $user_id ) {
			return false;
		}
		$package_count = intval(get_post_meta($user_package_id, $prefix.'package_count', true)) + 1;
		
		update_post_meta($user_package_id, $prefix.'package_count', $package_count);
	}

	public static function get_packages_by_user( $user_id, $valid = true, $package_type = 'listing_package' ) {
		$prefix = WP_CARDEALER_WC_PAID_LISTINGS_PREFIX;
		$meta_query = array(
			array(
				'key'     => $prefix.'user_id',
				'value'   => $user_id,
				'compare' => '='
			)
		);
		if ( $package_type != 'all' ) {
			$meta_query[] = array(
				'key'     => $prefix.'package_type',
				'value'   => $package_type,
				'compare' => '='
			);
		}
		$query_args = array(
			'post_type' => 'listing_package',
			'post_status' => 'publish',
			'posts_per_page'   => -1,
			'order'            => 'asc',
			'orderby'          => 'menu_order',
			'meta_query' => $meta_query
		);

		$packages = get_posts($query_args);
		$return = array();
		if ( $valid && $packages ) {
			foreach ($packages as $package) {
				$package_count = get_post_meta($package->ID, $prefix.'package_count', true);
				$listing_limit = get_post_meta($package->ID, $prefix.'listing_limit', true);

				if ( $package_count < $listing_limit || $listing_limit == 0 ) {
					$return[] = $package;
				}
				
			}
		} else {
			$return = $packages;
		}
		return $return;
	}

	public static function get_listings_for_package( $user_package_id ) {
		$prefix = WP_CARDEALER_LISTING_PREFIX;
		
		$query_args = array(
			'post_type' => 'listing',
			'post_status' => 'publish',
			'posts_per_page'   => -1,
			'fields' => 'ids',
			'meta_query' => array(
				array(
					'key'     => $prefix.'user_package_id',
					'value'   => $user_package_id,
					'compare' => '='
				)
			)
		);
		$posts = get_posts( $query_args );

		return $posts;
	}

	public static function is_woocommerce_subscriptions_pre( $version ) {
		if ( class_exists( 'WC_Subscriptions' ) && version_compare( WC_Subscriptions::$version, $version, '<' ) ) {
			return true;
		}

		return false;
	}
	
}

