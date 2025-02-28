<?php
if ( ! defined( 'ABSPATH' ) ) {
    exit; // Exit if accessed directly
}
if ( $user_packages ) : ?>
	<div class="widget widget-your-packages">
		<h2 class="widget-title"><?php esc_html_e( 'Your Packages', 'wp-cardealer-wc-paid-listings' ); ?></h2>
		<div class="row">
			<?php
				$prefix = WP_CARDEALER_WC_PAID_LISTINGS_PREFIX;
			foreach ( $user_packages as $key => $package ) :
				$package_count = get_post_meta($package->ID, $prefix.'package_count', true);
				$listing_limit = get_post_meta($package->ID, $prefix.'listing_limit', true);
				$listing_duration = get_post_meta($package->ID, $prefix.'listing_duration', true);
				$feature_listings = get_post_meta($package->ID, $prefix.'feature_listings', true);
			?>
				<div class="col-sm-4 col-xs-12 user-listing-package">
					<h3 class="title"><?php echo trim($package->post_title); ?></h3>
					<ul class="package-information">
						<?php
						if ( $listing_limit ) {
							?>
							<li>
								<?php echo sprintf( _n( '%s listing posted out of %d', '%s listings posted out of %d', $package_count, 'wp-cardealer-wc-paid-listings' ), $package_count, $listing_limit ); ?>
							</li>
							<?php
						} else {
							?>
							<li>
								<?php echo sprintf( _n( '%s listing posted', '%s listings posted', $package_count, 'wp-cardealer-wc-paid-listings' ), $package_count ); ?>
							</li>
							<?php
						}

						if ( $listing_duration ) {
							?>
							<li>
								<?php echo sprintf( _n( 'listed for %s day', 'listed for %s days', $listing_duration, 'wp-cardealer-wc-paid-listings' ), $listing_duration ); ?>
							</li>
							<?php
						}

						?>
						<li>
							<?php echo sprintf(__( 'Featured Listing: %s', 'wp-cardealer-wc-paid-listings' ), $feature_listings ? __( 'Yes', 'wp-cardealer-wc-paid-listings' ) : __( 'No', 'wp-cardealer-wc-paid-listings' )  ); ?>
						</li>
					</ul>

					<button class="btn btn-danger" type="submit" name="wcdwpl_listing_package" value="user-<?php echo esc_attr($package->ID); ?>">
						<?php esc_html_e('Add Listing', 'wp-cardealer-wc-paid-listings') ?>
					</button>

				</div>
			<?php endforeach; ?>
		</div>
		
	</div>
<?php endif; ?>