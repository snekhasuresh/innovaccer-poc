(function ($) {
    "use strict";

    if (!$.wjbWcAdminExtensions)
        $.wjbWcAdminExtensions = {};
    
    function WJBWCAdminMainCore() {
        var self = this;
        self.init();
    };

    WJBWCAdminMainCore.prototype = {
        /**
         *  Initialize
         */
        init: function() {
            var self = this;

            self.mixes();
        },
        mixes: function() {
            var self = this;

            var val_package_type = $('#_listing_package_package_type').val();
            self.changePackageTypeFn(val_package_type);
            $('#_listing_package_package_type').on('change', function() {
                var val_package_type = $(this).val();
                self.changePackageTypeFn(val_package_type);
            });

            self.productPackageTypeFn();
        },
        changePackageTypeFn: function(val_package_type) {
            if ( val_package_type == 'listing_package' ) {
                $('#_listing_package_listing_package').css({'display': 'block'});
                //
            } else {
                $('#_listing_package_listing_package').css({'display': 'none'});
            }
        },
        productPackageTypeFn: function() {
            $('._tax_status_field').closest('div').addClass( 'show_if_listing_package show_if_listing_package_subscription' );
            $('.show_if_subscription, .grouping').addClass( 'show_if_listing_package_subscription' );
            $('#product-type').change();

            $('#_listing_package_subscription_type').change(function(){
                if ( $(this).val() === 'listing' ) {
                    $('#_listings_duration').closest('.form-field').hide().val('');
                } else {
                    $('#_listings_duration').closest('.form-field').show();
                }
            }).change();
        },
    }

    $.wjbWcAdminMainCore = WJBWCAdminMainCore.prototype;
    
    $(document).ready(function() {
        // Initialize script
        new WJBWCAdminMainCore();
    });
    
})(jQuery);

