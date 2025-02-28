<?php
/**
 * product type: package
 *
 * @package    wp-cardealer-wc-paid-listings
 * @author     Habq 
 * @license    GNU General Public License, version 3
 */

if ( ! defined( 'ABSPATH' ) ) {
	exit;
}

function wp_cardealer_wc_paid_listings_register_package_product_type() {
	class WP_CarDealer_Wc_Paid_Listings_Product_Type_Package extends WC_Product_Simple {
		
		public function __construct( $product ) {
			parent::__construct( $product );
		}

		public function get_type() {
	        return 'listing_package';
	    }

	    public function is_sold_individually() {
			return apply_filters( 'wp_cardealer_wc_paid_listings_' . $this->product_type . '_is_sold_individually', true );
		}

		public function is_purchasable() {
			return true;
		}

		public function is_virtual() {
			return true;
		}
	}

	if ( class_exists( 'WC_Subscriptions' ) ) {
		class WP_CarDealer_Wc_Paid_Listings_Product_Type_Package_Subscription extends WC_Product_Subscription {
		
			public function __construct( $product ) {
				parent::__construct( $product );
			}

			public function get_type() {
		        return 'listing_package_subscription';
		    }

		    public function is_type( $type ) {
				return ( 'listing_package_subscription' == $type || ( is_array( $type ) && in_array( 'listing_package_subscription', $type ) ) ) ? true : parent::is_type( $type );
			}
			
			public function add_to_cart_url() {
				$url = $this->is_in_stock() ? remove_query_arg( 'added-to-cart', add_query_arg( 'add-to-cart', $this->id ) ) : get_permalink( $this->id );

				return apply_filters( 'woocommerce_product_add_to_cart_url', $url, $this );
			}
			
		    public function is_sold_individually() {
				return apply_filters( 'wp_cardealer_wc_paid_listings_' . $this->product_type . '_is_sold_individually', true );
			}

			public function is_purchasable() {
				return true;
			}

			public function is_virtual() {
				return true;
			}
		}
	}
}

add_action( 'init', 'wp_cardealer_wc_paid_listings_register_package_product_type' );


function wp_cardealer_wc_paid_listings_add_listing_package_product( $types ) {
	$types[ 'listing_package' ] = __( 'Listing Package', 'wp-cardealer-wc-paid-listings' );
	if ( class_exists( 'WC_Subscriptions' ) ) {
		$types['listing_package_subscription'] = __( 'Listing Package Subscription', 'wp-cardealer-wc-paid-listings' );
	}
	return $types;
}

add_filter( 'product_type_selector', 'wp_cardealer_wc_paid_listings_add_listing_package_product' );

function wp_cardealer_wc_paid_listings_woocommerce_product_class( $classname, $product_type ) {

    if ( $product_type == 'listing_package' ) { // notice the checking here.
        $classname = 'WP_CarDealer_Wc_Paid_Listings_Product_Type_Package';
    }

    if ( class_exists( 'WC_Subscriptions' ) ) {
	    if ( $product_type == 'listing_package_subscription' ) { // notice the checking here.
	        $classname = 'WP_CarDealer_Wc_Paid_Listings_Product_Type_Package_Subscription';
	    }
    }

    return $classname;
}

add_filter( 'woocommerce_product_class', 'wp_cardealer_wc_paid_listings_woocommerce_product_class', 10, 2 );


/**
 * Show pricing fields for package product.
 */
function wp_cardealer_wc_paid_listings_package_custom_js() {

	if ( 'product' != get_post_type() ) {
		return;
	}

	?><script type='text/javascript'>
		jQuery( document ).ready( function() {
			// listing package
			jQuery('.product_data_tabs .general_tab').show();
        	jQuery('#general_product_data .pricing').addClass('show_if_listing_package').show();
			jQuery('.inventory_options').addClass('show_if_listing_package').show();
			jQuery('.inventory_options').addClass('show_if_listing_package').show();
            jQuery('#inventory_product_data ._manage_stock_field').addClass('show_if_listing_package').show();
            jQuery('#inventory_product_data ._sold_individually_field').parent().addClass('show_if_listing_package').show();
            jQuery('#inventory_product_data ._sold_individually_field').addClass('show_if_listing_package').show();

		});
	</script><?php
}
add_action( 'admin_footer', 'wp_cardealer_wc_paid_listings_package_custom_js' );


function wp_cardealer_wc_paid_listings_woocommerce_subscription_product_types( $types ) {
	$types[] = 'listing_package_subscription';
	return $types;
}
add_filter( 'woocommerce_subscription_product_types', 'wp_cardealer_wc_paid_listings_woocommerce_subscription_product_types' );


function wp_cardealer_wc_paid_listings_package_options_product_tab_content() {
	global $post;
	$post_id = $post->ID;
	?>
	<!-- Listing Package -->
	<div class="options_group show_if_listing_package show_if_listing_package_subscription">
		<?php
			if ( class_exists( 'WC_Subscriptions' ) ) {
				woocommerce_wp_select( array(
					'id' => '_listing_package_subscription_type',
					'label' => __( 'Subscription Type', 'wp-cardealer-wc-paid-listings' ),
					'description' => __( 'Choose how subscriptions affect this package', 'wp-cardealer-wc-paid-listings' ),
					'value' => get_post_meta( $post_id, '_listing_package_subscription_type', true ),
					'desc_tip' => true,
					'options' => array(
						'package' => __( 'Link the subscription to the package (renew listing limit every subscription term)', 'wp-cardealer-wc-paid-listings' ),
						'listing' => __( 'Link the subscription to posted listings (renew posted listings every subscription term)', 'wp-cardealer-wc-paid-listings' )
					),
					'wrapper_class' => 'show_if_listing_package_subscription',
				) );
			}

			woocommerce_wp_checkbox( array(
				'id' 		=> '_feature_listings',
				'label' 	=> __( 'Feature Listings?', 'wp-cardealer-wc-paid-listings' ),
				'description'	=> __( 'Feature this listing - it will be styled differently and sticky.', 'wp-cardealer-wc-paid-listings' ),
			) );
			woocommerce_wp_text_input( array(
				'id'			=> '_listings_limit',
				'label'			=> __( 'Listings Limit', 'wp-cardealer-wc-paid-listings' ),
				'desc_tip'		=> 'true',
				'description'	=> __( 'The number of listings a user can post with this package', 'wp-cardealer-wc-paid-listings' ),
				'type' 			=> 'number',
			) );
			woocommerce_wp_text_input( array(
				'id'			=> '_listings_duration',
				'label'			=> __( 'Listings Duration (Days)', 'wp-cardealer-wc-paid-listings' ),
				'desc_tip'		=> 'true',
				'description'	=> __( 'The number of days that the listings will be active', 'wp-cardealer-wc-paid-listings' ),
				'type' 			=> 'number',
			) );

			do_action('wp_cardealer_wc_paid_listings_package_options_product_tab_content');
		?>
	</div>

	<?php
}
add_action( 'woocommerce_product_options_general_product_data', 'wp_cardealer_wc_paid_listings_package_options_product_tab_content' );

/**
 * Save the Listing Package custom fields.
 */
function wp_cardealer_wc_paid_listings_save_package_option_field( $post_id ) {
	$feature_listings = isset( $_POST['_feature_listings'] ) ? 'yes' : 'no';
	update_post_meta( $post_id, '_feature_listings', $feature_listings );
	
	if ( isset( $_POST['_listing_package_subscription_type'] ) ) {
		update_post_meta( $post_id, '_listing_package_subscription_type', sanitize_text_field( $_POST['_listing_package_subscription_type'] ) );
	}

	if ( isset( $_POST['_listings_limit'] ) ) {
		update_post_meta( $post_id, '_listings_limit', sanitize_text_field( $_POST['_listings_limit'] ) );
	}

	if ( isset( $_POST['_listings_duration'] ) ) {
		update_post_meta( $post_id, '_listings_duration', sanitize_text_field( $_POST['_listings_duration'] ) );
	}
}
add_action( 'woocommerce_process_product_meta_listing_package', 'wp_cardealer_wc_paid_listings_save_package_option_field'  );
add_action( 'woocommerce_process_product_meta_listing_package_subscription', 'wp_cardealer_wc_paid_listings_save_package_option_field'  );