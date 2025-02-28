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

class WP_CarDealer_Wc_Paid_Listings_Order {

	
	public static function init() {
		add_action( 'woocommerce_thankyou', array( __CLASS__, 'woocommerce_thankyou' ), 5 );

		// Change Order Statuses
		add_action( 'woocommerce_order_status_processing', array( __CLASS__, 'order_paid' ) );
		add_action( 'woocommerce_order_status_completed', array( __CLASS__, 'order_paid' ) );

		// Delete User
		add_action( 'delete_user', array( __CLASS__, 'delete_user_packages' ) );
	}

	
	public static function woocommerce_thankyou( $order_id ) {
		global $wp_post_types;

		$order = wc_get_order( $order_id );

		foreach ( $order->get_items() as $item ) {
			if ( isset( $item['listing_id'] ) ) {

				update_post_meta( $item['job_id'], WP_CARDEALER_LISTING_PREFIX.'order_id', $order_id );

				switch ( get_post_status( $item['listing_id'] ) ) {
					case 'pending' :
						echo wpautop( sprintf( __( '%s has been submitted successfully and will be visible once approved.', 'wp-cardealer-wc-paid-listings' ), get_the_title( $item['listing_id'] ) ) );
					break;
					case 'pending_payment' :
					case 'expired' :
						echo wpautop( sprintf( __( '%s has been submitted successfully and will be visible once payment has been confirmed.', 'wp-cardealer-wc-paid-listings' ), get_the_title( $item['listing_id'] ) ) );
					break;
					default :
						echo wpautop( sprintf( __( '%s has been submitted successfully.', 'wp-cardealer-wc-paid-listings' ), get_the_title( $item['listing_id'] ) ) );
					break;
				}

				echo '<p class="listing-submit-done-paid-listing-actions">';

				if ( 'publish' === get_post_status( $item['listing_id'] ) ) {
					echo '<a class="button" href="' . get_permalink( $item['listing_id'] ) . '">' . __( 'View Listing', 'wp-cardealer-wc-paid-listings' ) . '</a> ';
				} elseif ( wp_cardealer_get_option( 'my_listings_page_id' ) ) {
					echo '<a class="button" href="' . get_permalink( wp_cardealer_get_option( 'my_listings_page_id' ) ) . '">' . __( 'View Dashboard', 'wp-cardealer-wc-paid-listings' ) . '</a> ';
				}

				echo '</p>';

			}
		}
	}

	
	public static function order_paid( $order_id ) {
		$order = wc_get_order( $order_id );

		if ( get_post_meta( $order_id, 'wp_cardealer_wc_paid_listings_packages_processed', true ) ) {
			return;
		}
		foreach ( $order->get_items() as $item ) {
			$product = wc_get_product( $item['product_id'] );

			if ( $product->is_type( array( 'listing_package' ) ) && $order->get_customer_id() ) {

				// create packages for user
				for ( $i = 0; $i < $item['qty']; $i ++ ) {
					$user_package_id = WP_CarDealer_Wc_Paid_Listings_Mixes::create_user_package( $order->get_customer_id(), $product->get_id(), $order_id );
				}

				// Approve listing with new package
				if ( isset( $item['listing_id'] ) ) {
					$listing = get_post( $item['listing_id'] );

					if ( in_array( $listing->post_status, array( 'pending_payment', 'expired' ) ) ) {
						WP_CarDealer_Wc_Paid_Listings_Mixes::approve_listing_with_package( $listing->ID, $order->get_customer_id(), $user_package_id );
					}
				}
			}
		}
		
		update_post_meta( $order_id, 'wp_cardealer_wc_paid_listings_packages_processed', true );
	}

	
	public static function delete_user_packages( $user_id ) {
		if ( $user_id ) {
			$packages = get_posts(array(
				'post_type' => array('listing_package'),
				'meta_query' => array(
					array(
						'key'     => WP_CARDEALER_WC_PAID_LISTINGS_PREFIX.'user_id',
						'value'   => $user_id,
						'compare' => '='
					)
				)
			));
			if ( !empty($packages) ) {
				foreach ($packages as $package) {
					wp_delete_post($package->ID, true);
				}
			}
		}

	}
}
WP_CarDealer_Wc_Paid_Listings_Order::init();
