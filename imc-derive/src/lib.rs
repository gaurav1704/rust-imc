use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(CriticalKey)]
pub fn derive_critical_key(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let expanded = quote! {
        #[automatically_derived]
        impl #impl_generics CriticalKey for #name #ty_generics #where_clause {
            fn channel() -> &'static str {
                concat!(module_path!(), "::", stringify!(#name))
            }
        }
    };

    expanded.into()
}
