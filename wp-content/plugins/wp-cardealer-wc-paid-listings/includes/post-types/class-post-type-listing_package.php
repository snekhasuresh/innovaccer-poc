<?php
/**
 * Package
 *
 * @package    wp-cardealer-wc-paid-listings
 * @author     Habq 
 * @license    GNU General Public License, version 3
 */
 
if ( ! defined( 'ABSPATH' ) ) {
  exit;
}

class WP_CarDealer_Wc_Paid_Listings_Post_Type_Packages {

  	public static function init() {
    	add_action( 'init', array( __CLASS__, 'register_post_type' ) );

    	add_action( 'cmb2_meta_boxes', array( __CLASS__, 'fields' ) );

    	add_filter( 'manage_edit-listing_package_columns', array( __CLASS__, 'custom_columns' ) );
		add_action( 'manage_listing_package_posts_custom_column', array( __CLASS__, 'custom_columns_manage' ) );

		add_action('restrict_manage_posts', array( __CLASS__, 'filter_listing_package_by_type' ));
  	}

  	public static function register_post_type() {
	    $labels = array(
			'name'                  => esc_html__( 'User Package', 'wp-cardealer-wc-paid-listings' ),
			'singular_name'         => esc_html__( 'User Package', 'wp-cardealer-wc-paid-listings' ),
			'add_new'               => esc_html__( 'Add New Package', 'wp-cardealer-wc-paid-listings' ),
			'add_new_item'          => esc_html__( 'Add New Package', 'wp-cardealer-wc-paid-listings' ),
			'edit_item'             => esc_html__( 'Edit Package', 'wp-cardealer-wc-paid-listings' ),
			'new_item'              => esc_html__( 'New Package', 'wp-cardealer-wc-paid-listings' ),
			'all_items'             => esc_html__( 'User Packages', 'wp-cardealer-wc-paid-listings' ),
			'view_item'             => esc_html__( 'View Package', 'wp-cardealer-wc-paid-listings' ),
			'search_items'          => esc_html__( 'Search Package', 'wp-cardealer-wc-paid-listings' ),
			'not_found'             => esc_html__( 'No Packages found', 'wp-cardealer-wc-paid-listings' ),
			'not_found_in_trash'    => esc_html__( 'No Packages found in Trash', 'wp-cardealer-wc-paid-listings' ),
			'parent_item_colon'     => '',
			'menu_name'             => esc_html__( 'User Packages', 'wp-cardealer-wc-paid-listings' ),
	    );

	    register_post_type( 'listing_package',
	      	array(
		        'labels'            => apply_filters( 'wp_cardealer_wc_paid_listings_postype_package_fields_labels' , $labels ),
		        'supports'          => array( 'title' ),
		        'public'            => true,
		        'has_archive'       => false,
		        'publicly_queryable' => false,
		        'show_in_menu'		=> 'edit.php?post_type=listing',
	      	)
	    );
  	}
	
	public static function package_types() {
		return apply_filters('wp-cardealer-wc-paid-listings-package-types', array(
			'listing_package' => __('Listing Package', 'wp-cardealer-wc-paid-listings'),
		));
	}

	public static function get_packages() {
		$packages = array( '' => __('Choose a package', 'wp-cardealer-wc-paid-listings') );
		$product_packages = WP_CarDealer_Wc_Paid_Listings_Mixes::get_listing_package_products();
		if ( !empty($product_packages) ) {
			foreach ($product_packages as $product) {
				$packages[$product->ID] = $product->post_title;
			}
		}
		return $packages;
	}

  	public static function fields( array $metaboxes ) {
		$prefix = WP_CARDEALER_WC_PAID_LISTINGS_PREFIX;


		$package_types = array_merge(array('' => __('Choose package type', 'wp-cardealer-wc-paid-listings')), self::package_types());
		$metaboxes[ $prefix . 'general' ] = array(
			'id'                        => $prefix . 'general',
			'title'                     => __( 'General Options', 'wp-cardealer-wc-paid-listings' ),
			'object_types'              => array( 'listing_package' ),
			'context'                   => 'normal',
			'priority'                  => 'high',
			'show_names'                => true,
			'show_in_rest'				=> true,
			'fields'                    => array(
				array(
					'name'              => __( 'Order Id', 'wp-cardealer-wc-paid-listings' ),
					'id'                => $prefix . 'order_id',
					'type'              => 'text',
				),
				array(
					'name'              => __( 'User id', 'wp-cardealer-wc-paid-listings' ),
					'id'                => $prefix . 'user_id',
					'type'              => 'text',
				),
				array(
					'name'              => __( 'Package Type', 'wp-cardealer-wc-paid-listings' ),
					'id'                => $prefix . 'package_type',
					'type'              => 'select',
					'options'			=> $package_types
				),
			),
		);

		$packages = self::get_packages();
		$metaboxes[ $prefix . 'listing_package' ] = array(
			'id'                        => $prefix . 'listing_package',
			'title'                     => __( 'Listing Package Options', 'wp-cardealer-wc-paid-listings' ),
			'object_types'              => array( 'listing_package' ),
			'context'                   => 'normal',
			'priority'                  => 'high',
			'show_names'                => true,
			'show_in_rest'				=> true,
			'fields'                    => array(
				array(
					'name'              => __( 'Package', 'wp-cardealer-wc-paid-listings' ),
					'id'                => $prefix . 'product_id',
					'type'              => 'select',
					'options'			=> $packages
				),
				array(
					'name'              => __( 'Package Count', 'wp-cardealer-wc-paid-listings' ),
					'id'                => $prefix . 'package_count',
					'type'              => 'text',
					'attributes' 	    => array(
						'type' 				=> 'number',
						'min'				=> 0,
						'pattern' 			=> '\d*',
					)
				),
				array(
					'name'              => __( 'Featured Listings', 'wp-cardealer-wc-paid-listings' ),
					'id'                => $prefix . 'feature_listings',
					'type'              => 'checkbox',
					'desc'				=> __( 'Feature this listing - it will be styled differently and sticky.', 'wp-cardealer-wc-paid-listings' ),
				),
				array(
					'name'              => __( 'Listing duration', 'wp-cardealer-wc-paid-listings' ),
					'id'                => $prefix . 'listing_duration',
					'type'              => 'text',
					'attributes' 	    => array(
						'type' 				=> 'number',
						'min'				=> 0,
						'pattern' 			=> '\d*',
					),
					'desc'				=> __( 'The number of days that the listings will be active', 'wp-cardealer-wc-paid-listings' ),
				),
				array(
					'name'              => __( 'Listings limit', 'wp-cardealer-wc-paid-listings' ),
					'id'                => $prefix . 'listing_limit',
					'type'              => 'text',
					'attributes' 	    => array(
						'type' 				=> 'number',
						'min'				=> 0,
						'pattern' 			=> '\d*',
					),
					'desc'				=> __( 'The number of listings a user can post with this package', 'wp-cardealer-wc-paid-listings' ),
				),
			),
		);

		return $metaboxes;
	}


	/**
	 * Custom admin columns for post type
	 *
	 * @access public
	 * @return array
	 */
	public static function custom_columns() {
		$fields = array(
			'cb' 				=> '<input type="checkbox" />',
			'title' 			=> __( 'Title', 'wp-cardealer' ),
			'package_type' 		=> __( 'Package Type', 'wp-cardealer' ),
			'author' 			=> __( 'Author', 'wp-cardealer' ),
			'date' 				=> __( 'Date', 'wp-cardealer' ),
		);
		return $fields;
	}

	/**
	 * Custom admin columns implementation
	 *
	 * @access public
	 * @param string $column
	 * @return array
	 */
	public static function custom_columns_manage( $column ) {
		global $post;
		$prefix = WP_CARDEALER_WC_PAID_LISTINGS_PREFIX;
		switch ( $column ) {
			case 'package_type':
				$package_type = get_post_meta($post->ID, $prefix.'package_type', true );
				$package_types = self::package_types();
				if ( !empty($package_types[$package_type]) ) {
					echo $package_types[$package_type];
				} else {
					echo '-';
				}
				break;
		}
	}

	public static function filter_listing_package_by_type() {
		global $typenow;
		if ( $typenow == 'listing_package') {
			// categories
			$selected = isset($_GET['package_type']) ? $_GET['package_type'] : '';
			$package_types = self::package_types();
			if ( ! empty( $package_types ) ){
				?>
				<select name="package_type">
					<option value=""><?php esc_html_e('All package types', 'wp-cardealer'); ?></option>
					<?php
					foreach ($package_types as $key => $title) {
						?>
						<option value="<?php echo esc_attr($key); ?>" <?php selected($selected, $key); ?>><?php echo esc_html($title); ?></option>
						<?php
					}
				?>
				</select>
				<?php
			}
		}
	}

}

WP_CarDealer_Wc_Paid_Listings_Post_Type_Packages::init();