<?php
if ( ! defined( 'ABSPATH' ) ) {
    exit; // Exit if accessed directly
}

$form      = WP_CarDealer_Submit_Form::get_instance();
$listing_id    = $form->get_listing_id();
$step      = $form->get_step();
$form_name = $form->get_form_name();

$user_id = get_current_user_id();
$user_packages = WP_CarDealer_Wc_Paid_Listings_Mixes::get_packages_by_user($user_id, true);
$packages = WP_CarDealer_Wc_Paid_Listings_Submit_Form::get_products();

?>
<form method="post" id="listing_package_selection">
	<?php if ( empty($listing_id) || WP_CarDealer_User::is_user_can_edit_listing( $listing_id ) ) { ?>
		<div class="listing_listing_packages_title">
			<input type="hidden" name="listing_id" value="<?php echo esc_attr( $listing_id ); ?>" />

			<input type="hidden" name="<?php echo esc_attr($form_name); ?>" value="<?php echo esc_attr($form_name); ?>">
			<input type="hidden" name="submit_step" value="<?php echo esc_attr($step); ?>">
			<input type="hidden" name="object_id" value="<?php echo esc_attr($listing_id); ?>">

			<?php wp_nonce_field('wp-cardealer-listing-submit-package-nonce', 'security-listing-submit-package'); ?>

			<h2><?php esc_html_e( 'Choose a package', 'wp-cardealer-wc-paid-listings' ); ?></h2>
		</div>
		<div class="listing_types">
			<?php echo WP_CarDealer_Wc_Paid_Listings_Template_Loader::get_template_part('user-packages', array('user_packages' => $user_packages) ); ?>
			<?php echo WP_CarDealer_Wc_Paid_Listings_Template_Loader::get_template_part('packages', array('packages' => $packages) ); ?>
		</div>
	<?php } else { ?>
		<div class="text-warning">
			<?php esc_html_e('Sorry, you can\'t post a listing.', 'wp-cardealer-wc-paid-listings'); ?>
		</div>
	<?php } ?>
</form>