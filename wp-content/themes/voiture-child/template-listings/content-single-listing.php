<?php
if (! defined('ABSPATH')) {
	exit;
}
global $post;

wp_enqueue_script('sticky-kit');
?>

<?php do_action('wp_cardealer_before_listing_detail', $post->ID); ?>

<article id="post-<?php the_ID(); ?>" <?php post_class('listing-single-layout listing-single-v1'); ?> style="background-color: white; padding:0px !important">

	<div class="<?php echo apply_filters('voiture_listing_content_class', 'container'); ?>">

		<?php voiture_render_breadcrumbs_simple(); ?>

		<!-- Content header -->
		<?php echo WP_CarDealer_Template_Loader::get_template_part('single-listing/header'); ?>

		<?php echo WP_CarDealer_Template_Loader::get_template_part('single-listing/tabs'); ?>

		<!-- Main content -->
		<div class="content-listing-detail">

			<div style="background-color: white;" class="row listing-v-wrapper">
				<?php echo do_shortcode('[car_image_gallery]'); ?>
				<div class="col-xs-12  col-md-<?php echo esc_attr(is_active_sidebar('listing-single-sidebar') ? 9 : 9); ?> col-lg-<?php echo esc_attr(is_active_sidebar('listing-single-sidebar') ? 9 : 9); ?>">
					<?php do_action('wp_cardealer_before_listing_content', $post->ID); ?>


					<?php //echo WP_CarDealer_Template_Loader::get_template_part('single-listing/specs'); 
					?>

					<?php //echo WP_CarDealer_Template_Loader::get_template_part( 'single-listing/used-cars' ); 
					?>
					<?php
					// echo WP_CarDealer_Template_Loader::get_template_part('single-listing/pros-and-cons');
					?>
					<div>
						<?php
						// echo WP_CarDealer_Template_Loader::get_template_part('single-listing/variants');
						echo do_shortcode('[single_listing_variants]');
						?>

					</div>

					<?php echo do_shortcode('[ownership_cost]'); ?>


					<?php
					echo do_shortcode('[single_listing_car_news]');
					?>

					<?php
					echo do_shortcode('[single_listing_videos]');
					// echo WP_CarDealer_Template_Loader::get_template_part('single-listing/videos');
					?>


					<?php echo do_shortcode('[mega_image_gallery]'); ?>


					<?php
					echo do_shortcode('[single_listing_car_overview]');

					// echo WP_CarDealer_Template_Loader::get_template_part('single-listing/car-overview');
					?>

					<?php
					echo WP_CarDealer_Template_Loader::get_template_part('single-listing/fuel-consumption');
					?>
					<?php
					// echo WP_CarDealer_Template_Loader::get_template_part('single-listing/car-color');
					// echo do_shortcode('[single_listing_car_color]');

					?>
					<?php
					// echo WP_CarDealer_Template_Loader::get_template_part('single-listing/car-comparision');
					// echo do_shortcode('[single_listing_car_comparison]');
					?>

					<?php echo do_shortcode('[nearest_honda_dealers]'); ?>

					<?php
					// echo WP_CarDealer_Template_Loader::get_template_part('single-listing/recommended-cars');
					echo do_shortcode('[single_listing_recommended_cars]');

					?>

					<?php
					// echo do_shortcode('[popular_car_brands]');
					// echo WP_CarDealer_Template_Loader::get_template_part('single-listing/brand-name');
					?>

					<?php
					echo WP_CarDealer_Template_Loader::get_template_part('single-listing/faq');
					?>


					<?php do_action('wp_cardealer_after_listing_content', $post->ID); ?>

				</div>

				<div class="col-xs-12  col-md-<?php echo esc_attr(is_active_sidebar('listing-single-sidebar') ? 3 : 3); ?> col-lg-<?php echo esc_attr(is_active_sidebar('listing-single-sidebar') ? 3 : 3); ?>">
					<?php echo do_shortcode('[add-new-car]'); ?>
					<?php echo do_shortcode('[car_competitors]'); ?>


				</div>

				<?php if (is_active_sidebar('listing-single-sidebar')): ?>
					<!-- <div class="col-xs-12 col-md-4 col-lg-4 sidebar-listing sidebar-wrapper sticky-this">
				   		<div class="sidebar sidebar-right">
					   		<?php //dynamic_sidebar( 'listing-single-sidebar' ); 

								?>
				   		</div>
				   	</div> -->
				<?php endif; ?>
				<div class="col-xs-12 listing-detail-main col-md-<?php echo esc_attr(is_active_sidebar('listing-single-sidebar') ? 12 : 12); ?> col-lg-<?php echo esc_attr(is_active_sidebar('listing-single-sidebar') ? 12 : 12); ?>">

					<?php

					// echo WP_CarDealer_Template_Loader::get_template_part( 'single-listing/car-colors' );
					?>



					<?php if (WP_CarDealer_Review::review_enable()) { ?>
						<?php comments_template(); ?>
					<?php } ?>

				</div>
			</div>
		</div>
	</div>
	<?php
	if (voiture_get_config('show_listing_related', true)) {
		echo WP_CarDealer_Template_Loader::get_template_part('single-listing/related');
	}

	?>

</article><!-- #post-## -->


<?php do_action('wp_cardealer_after_listing_detail', $post->ID); ?>