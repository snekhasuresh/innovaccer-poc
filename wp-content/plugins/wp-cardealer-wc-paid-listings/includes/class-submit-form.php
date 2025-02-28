<?php
/**
 * Submit Form
 *
 * @package    wp-cardealer-wc-paid-listings
 * @author     Habq 
 * @license    GNU General Public License, version 3
 */
 
if ( ! defined( 'ABSPATH' ) ) {
	exit; // Exit if accessed directly
}

class WP_CarDealer_Wc_Paid_Listings_Submit_Form {
	
	public static $package_id = 0;
	public static $listing_user_package;
	public static $is_user_package = false;

	public static function init() {
		add_filter( 'wp_cardealer_submit_listing_steps',  array( __CLASS__, 'submit_listing_steps' ), 5, 1 );

		// get listing package
		if ( ! empty( $_POST['wcdwpl_listing_package'] ) ) {
			if ( is_numeric( $_POST['wcdwpl_listing_package'] ) ) {
				self::$package_id = absint( $_POST['wcdwpl_listing_package'] );
				self::$is_user_package = false;
			} else {
				self::$package_id = absint( substr( $_POST['wcdwpl_listing_package'], 5 ) );
				self::$is_user_package = true;
			}
		} elseif ( ! empty( $_POST['wcdwpl_listing_user_package'] ) ) {
			if ( is_numeric( $_POST['wcdwpl_listing_user_package'] ) ) {
				self::$package_id = absint( $_POST['wcdwpl_listing_user_package'] );
				self::$is_user_package = true;
			}
		} elseif ( ! empty( $_COOKIE['chosen_package_id'] ) ) {
			self::$package_id = absint( $_COOKIE['chosen_package_id'] );
			self::$is_user_package = absint( $_COOKIE['chosen_package_is_user_package'] ) === 1;
		}

		if ( empty(self::$package_id) ) {
			$listing_id = ! empty( $_REQUEST['listing_id'] ) ? absint( $_REQUEST['listing_id'] ) : 0;
			if ( !empty($listing_id) ) {
				$user_package_id = get_post_meta( $listing_id, '_listing_user_package_id', true );
				$package_id = get_post_meta( $listing_id, '_listing_package_id', true );

				if ( !empty($user_package_id) ) {
					self::$package_id = $user_package_id;
					self::$is_user_package = true;
				} elseif( !empty($package_id) ) {
					self::$package_id = $package_id;
					self::$is_user_package = false;
				}
			}
		}

		add_filter('wp-cardealer-get-listing-package-id', array( __CLASS__, 'get_package_id_post' ), 10, 2);

		add_action('wp-cardealer-before-preview-listing', array( __CLASS__, 'before_preview_listing' ));

		add_action( 'wp_loaded', array( __CLASS__, 'after_wp_loaded' ) );
	}

	public static function after_wp_loaded() {
		add_action('wp-cardealer-submit-listing-construct', array( __CLASS__, 'before_view_package' ));
	}

	public static function get_products() {
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
		$loop = new WP_Query($query_args);
		//$posts = get_posts( $query_args );
		$posts = $loop->posts;

		return $posts;
	}

	public static function submit_listing_steps($steps) {
		
		$packages = self::get_products();

		if ( !empty($packages) ) {
			$steps['wjb-choose-packages'] = array(
				'view'     => array( __CLASS__, 'choose_package' ),
				'handler'  => array( __CLASS__, 'choose_package_handler' ),
				'priority' => 1
			);

			$steps['wjb-process-packages'] = array(
				'name'     => '',
				'view'     => false,
				'handler'  => array( __CLASS__, 'process_package_handler' ),
				'priority' => 25
			);

			add_filter( 'wp_cardealer_submit_listing_post_status', array( __CLASS__, 'submit_listing_post_status' ), 10, 2 );
		}

		return $steps;
	}

	public static function submit_listing_post_status( $status, $listing ) {
		switch ( $listing->post_status ) {
			case 'preview' :
				return 'pending_payment';
			break;
			case 'expired' :
				return 'expired';
			break;
			default :
				return $status;
			break;
		}
		return $status;
	}

	public static function before_view_package($form) {
		if ( !empty($_GET['action']) && $_GET['action'] == 'continue' ) {

			$listing_id = $form->get_listing_id();
			$step = $form->get_step();

			if ( !empty($listing_id) && $step < 1 ) {
				$user_package_id = get_post_meta( $listing_id, WP_CARDEALER_LISTING_PREFIX.'user_package_id', true );
				$package_id = get_post_meta( $listing_id, WP_CARDEALER_LISTING_PREFIX.'package_id', true );

				if ( !empty($user_package_id) ) {
					self::$is_user_package = true;
					self::$package_id = $user_package_id;

					$form->set_step(1);
				} elseif( !empty($package_id) && in_array($package_id, self::get_cart_products()) ) {
					self::$is_user_package = false;
					self::$package_id = $package_id;
					
					$form->set_step(1);
				}
			}
		}
	}

	public static function get_cart_products() {
		$products = [];
		$carts = WC()->cart->get_cart();
		if ( !empty($carts) ) {
			foreach ($carts as $key => $cart_item) {
				$products[] = $cart_item['product_id'];
			}
		}
		return $products;
	}

	public static function choose_package($atts = array()) {
		echo WP_CarDealer_Wc_Paid_Listings_Template_Loader::get_template_part('choose-package-form', array('atts' => $atts) );
	}

	public static function get_package_id_post($product_id, $post_id) {
		if ( !empty($post_id) ) {
			if ( self::$package_id ) {
				if ( self::$is_user_package ) {
					$package_id = get_post_meta( self::$package_id, WP_CARDEALER_WC_PAID_LISTINGS_PREFIX . 'product_id', true );
					return $package_id;
				}
			} else {
				if ( metadata_exists('post', $post_id, WP_CARDEALER_LISTING_PREFIX.'package_id') ) {
					$package_id = get_post_meta( $post_id, WP_CARDEALER_LISTING_PREFIX.'package_id', true );
					return $package_id;
				}
			}
		} else {
			if ( self::$is_user_package ) {
				$package_id = get_post_meta( self::$package_id, WP_CARDEALER_WC_PAID_LISTINGS_PREFIX . 'product_id', true );
				return $package_id;
			}
		}

		return self::$package_id;
	}

	public static function get_package_id() {
		if ( self::$is_user_package ) {
			$package_id = get_post_meta( self::$package_id, WP_CARDEALER_WC_PAID_LISTINGS_PREFIX . 'product_id', true );
			return $package_id;
		}

		return self::$package_id;
	}


	public static function choose_package_handler() {

		$form = WP_CarDealer_Submit_Form::get_instance();

		if ( !isset( $_POST['security-listing-submit-package'] ) || ! wp_verify_nonce( $_POST['security-listing-submit-package'], 'wp-cardealer-listing-submit-package-nonce' )  ) {
			$form->add_error( esc_html__('Sorry, your nonce did not verify.', 'wp-cardealer-wc-paid-listings') );
			return;
		}

		$validation = self::validate_package();

		if ( is_wp_error( $validation ) ) {
			$form->add_error( $validation->get_error_message() );
			$form->set_step( array_search( 'wjb-choose-packages', array_keys( $form->get_steps() ) ) );
			return false;
		}
		
		wc_setcookie( 'chosen_package_id', self::$package_id );
		wc_setcookie( 'chosen_package_is_user_package', self::$is_user_package ? 1 : 0 );
		

		$form->next_step();
	}

	private static function validate_package() {
		if ( empty( self::$package_id ) ) {
			return new WP_Error( 'error', esc_html__( 'Invalid Package', 'wp-cardealer-wc-paid-listings' ) );
		} elseif ( self::$is_user_package ) {
			if ( ! WP_CarDealer_Wc_Paid_Listings_Mixes::package_is_valid( get_current_user_id(), self::$package_id ) ) {
				return new WP_Error( 'error', __( 'Invalid Package', 'wp-cardealer-wc-paid-listings' ) );
			}
		} else {
			$package = wc_get_product( self::$package_id );
			if ( empty($package) || ($package->get_type() != 'listing_package' && ! $package->is_type( 'listing_package_subscription' ) ) ) {
				return new WP_Error( 'error', esc_html__( 'Invalid Package', 'wp-cardealer-wc-paid-listings' ) );
			}

			// Don't let them buy the same subscription twice if the subscription is for the package
			if ( class_exists( 'WC_Subscriptions' ) && is_user_logged_in() && $package->is_type( 'listing_package_subscription' ) && 'package' === WP_CarDealer_Wc_Paid_Listings_Listing_Package_Subscription::get_package_subscription_type(self::$package_id) ) {
				if ( wcs_user_has_subscription( get_current_user_id(), self::$package_id, 'active' ) ) {
					return new WP_Error( 'error', __( 'You already have this subscription.', 'wp-cardealer-wc-paid-listings' ) );
				}
			}
		}

		return true;
	}

	public static function before_preview_listing($post) {
		if ( self::$package_id ) {
			if ( self::$is_user_package ) {
				$product_id = get_post_meta( self::$package_id, WP_CARDEALER_WC_PAID_LISTINGS_PREFIX . 'product_id', true );
				update_post_meta( $post->ID, WP_CARDEALER_LISTING_PREFIX.'user_package_id', self::$package_id );
				update_post_meta( $post->ID, WP_CARDEALER_LISTING_PREFIX.'package_id', $product_id );
			} else {
				update_post_meta( $post->ID, WP_CARDEALER_LISTING_PREFIX.'package_id', self::$package_id );
			}
		}
	}

	public static function process_package_handler() {
		$form = WP_CarDealer_Submit_Form::get_instance();
		$listing_id = $form->get_listing_id();
		$post_status = get_post_status( $listing_id );

		if ( $post_status == 'preview' ) {
			$update_listing = array(
				'ID' => $listing_id,
				'post_status' => 'pending_payment',
				'post_date' => current_time( 'mysql' ),
				'post_date_gmt' => current_time( 'mysql', 1 ),
				'post_author' => get_current_user_id(),
			);

			wp_update_post( $update_listing );
		}

		if ( self::$is_user_package ) {
			$product_id = get_post_meta(self::$package_id, WP_CARDEALER_WC_PAID_LISTINGS_PREFIX.'product_id', true);
			// Featured
			$feature_listings = get_post_meta(self::$package_id, WP_CARDEALER_WC_PAID_LISTINGS_PREFIX.'feature_listings', true );
			$featured = '';
			if ( !empty($feature_listings) && $feature_listings === 'yes' ) {
				$featured = 'on';
			}
			update_post_meta( $listing_id, WP_CARDEALER_LISTING_PREFIX. 'featured', $featured );
			//
			$listing_duration = get_post_meta(self::$package_id, WP_CARDEALER_WC_PAID_LISTINGS_PREFIX.'listing_duration', true );
			update_post_meta( $listing_id, WP_CARDEALER_LISTING_PREFIX.'package_duration', $listing_duration );
			update_post_meta( $listing_id, WP_CARDEALER_LISTING_PREFIX.'package_id', $product_id );
			update_post_meta( $listing_id, WP_CARDEALER_LISTING_PREFIX.'user_package_id', self::$package_id );

			$subscription_type = get_post_meta(self::$package_id, WP_CARDEALER_WC_PAID_LISTINGS_PREFIX.'subscription_type', true );
			if ( 'listing' === $subscription_type ) {
				update_post_meta( $listing_id, WP_CARDEALER_LISTING_PREFIX.'expiry_date', '' ); // Never expire automatically
			}

			// Approve the listing
			if ( in_array( get_post_status( $listing_id ), array( 'pending_payment', 'expired' ) ) ) {
				WP_CarDealer_Wc_Paid_Listings_Mixes::approve_listing_with_package( $listing_id, get_current_user_id(), self::$package_id );
			}

			
			do_action( 'wcdwpl_process_user_package_handler', self::$package_id, $listing_id );

			$form->next_step();
		} elseif ( self::$package_id ) {
			
			// Featured
			$feature_listings = get_post_meta(self::$package_id, '_feature_listings', true );
			$featured = '';
			if ( !empty($feature_listings) && $feature_listings === 'yes' ) {
				$featured = 'on';
			}
			update_post_meta( $listing_id, WP_CARDEALER_LISTING_PREFIX.'featured', $featured );
			//
			$listing_duration = get_post_meta(self::$package_id, '_listings_duration', true );
			update_post_meta( $listing_id, WP_CARDEALER_LISTING_PREFIX.'package_duration', $listing_duration );
			update_post_meta( $listing_id, WP_CARDEALER_LISTING_PREFIX.'package_id', self::$package_id );
			
			$subscription_type = get_post_meta(self::$package_id, '_listing_package_subscription_type', true );
			if ( 'listing' === $subscription_type ) {
				update_post_meta( $listing_id, WP_CARDEALER_LISTING_PREFIX.'expiry_date', '' ); // Never expire automatically
			}

			WC()->cart->add_to_cart( self::$package_id, 1, '', '', array(
				'listing_id' => $listing_id
			) );

			wc_add_to_cart_message( self::$package_id );

			// remove cookie
			wc_setcookie( 'chosen_package_id', '', time() - HOUR_IN_SECONDS );
			wc_setcookie( 'chosen_package_is_user_package', '', time() - HOUR_IN_SECONDS );

			do_action( 'wcdwpl_process_package_handler', self::$package_id, $listing_id );

			wp_redirect( get_permalink( wc_get_page_id( 'checkout' ) ) );
			exit;
		}
	}

}

WP_CarDealer_Wc_Paid_Listings_Submit_Form::init();